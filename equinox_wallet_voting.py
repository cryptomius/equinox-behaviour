import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import base64
import asyncio
import nest_asyncio
import pymongo
from typing import List, Dict, Any
import websockets
from concurrent.futures import ThreadPoolExecutor
import time
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
from pymongo import MongoClient

# Enable nested event loops
nest_asyncio.apply()

# Constants
INITIAL_BLOCK_HEIGHT = 17403644  # Dec 3rd 2024, 03:58:47+00:00
CONTRACT_ADDRESS = "neutron13l5nw6fh4xascjfyru5pe9w5ur8rp7svzl7hhkdh7jfdk3fup75quz9zy5"
WS_ENDPOINT = "wss://neutron-rpc.publicnode.com:443/websocket"
BLOCK_TIME_SECONDS = 6
DEFAULT_REST_ENDPOINT = "https://rest.neutron.nodestake.top"
COLLECTION_NAME_ESSENCE = 'wallet_essence_snapshots'
ESSENCE_SNAPSHOT_VALIDITY_HOURS = 24  # How long to consider a snapshot valid

# MongoDB setup
MONGO_URI = st.secrets["mongo"]["connection_string"]
MONGO_DB = 'equinox_voting'
COLLECTION_NAME = 'wallet_votes'

def setup_mongodb():
    """Setup MongoDB connection and collection"""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    # Define the schema for wallet votes
    wallet_votes_schema = {
        "bsonType": "object",
        "required": ["transaction_hash", "block_height", "block_timestamp", "sender", "votes"],
        "properties": {
            "transaction_hash": {
                "bsonType": "string",
                "description": "Hash of the transaction"
            },
            "block_height": {
                "bsonType": ["long", "int"],
                "description": "Block height of the transaction"
            },
            "block_timestamp": {
                "bsonType": ["string", "date", "null"],
                "description": "Timestamp of the block"
            },
            "sender": {
                "bsonType": "string",
                "description": "Address of the sender"
            },
            "votes": {
                "bsonType": "array",
                "description": "Array of vote allocations",
                "items": {
                    "bsonType": "object",
                    "required": ["lp_token", "weight"],
                    "properties": {
                        "lp_token": {
                            "bsonType": "string"
                        },
                        "weight": {
                            "bsonType": ["string", "double"],
                            "description": "Vote weight"
                        }
                    }
                }
            }
        }
    }
    
    # Only create collection if it doesn't exist
    if COLLECTION_NAME not in db.list_collection_names():
        db.create_collection(
            COLLECTION_NAME,
            validator={"$jsonSchema": wallet_votes_schema}
        )
    
    return db

# Initialize MongoDB database and collections
db = setup_mongodb()
wallet_votes_collection = db[COLLECTION_NAME]
wallet_essence_collection = db[COLLECTION_NAME_ESSENCE]

async def get_rpc_endpoints():
    """Fetch RPC endpoints from Cosmos Chain Registry"""
    try:
        response = requests.get("https://raw.githubusercontent.com/cosmos/chain-registry/master/neutron/chain.json")
        data = response.json()
        
        # Convert HTTP(S) endpoints to WebSocket endpoints
        rpc_endpoints = []
        for rpc in data.get("apis", {}).get("rpc", []):
            address = rpc.get("address", "")
            if address.startswith("http"):
                ws_address = address.replace("http", "ws") + "/websocket"
                rpc_endpoints.append(ws_address)
        
        # Add additional known endpoints
        additional_endpoints = [
            "wss://neutron-rpc.publicnode.com:443/websocket",
            "wss://neutron-rpc.lavenderfive.com:443/websocket"
        ]
        
        for endpoint in additional_endpoints:
            if endpoint not in rpc_endpoints:
                rpc_endpoints.append(endpoint)
        
        return rpc_endpoints
    except Exception as e:
        st.error(f"Error fetching RPC endpoints: {str(e)}")
        return []

async def test_rpc_endpoint(endpoint: str) -> tuple[str, int]:
    """Test an RPC endpoint and return the number of transactions it reports"""
    try:
        async with websockets.connect(endpoint, ping_interval=None) as websocket:
            # Query for the latest block
            await websocket.send(json.dumps({
                "jsonrpc": "2.0",
                "method": "block",
                "id": "1"
            }))
            response = await websocket.recv()
            data = json.loads(response)
            
            if 'result' in data:
                return endpoint, 1
            return endpoint, 0
    except:
        return endpoint, 0

async def find_best_rpc() -> str:
    """Find the best working RPC endpoint"""
    # Try the default endpoint first
    try:
        _, success = await test_rpc_endpoint(WS_ENDPOINT)
        if success:
            return WS_ENDPOINT
    except:
        pass
    
    # Fall back to testing other endpoints if default fails
    endpoints = await get_rpc_endpoints()
    
    # Create a progress bar
    progress_text = "Testing fallback RPC endpoints..."
    progress_bar = st.progress(0, text=progress_text)
    
    working_endpoints = []
    for i, endpoint in enumerate(endpoints):
        if endpoint != WS_ENDPOINT:  # Skip the default endpoint since we already tried it
            progress_bar.progress((i + 1) / len(endpoints), text=f"Testing {endpoint}...")
            endpoint, success = await test_rpc_endpoint(endpoint)
            if success:
                working_endpoints.append(endpoint)
    
    progress_bar.empty()
    return working_endpoints[0] if working_endpoints else None

async def fetch_transactions(progress_bar, start_height=None):
    """Fetch transactions from the blockchain"""
    # Get the latest processed block height from our database
    try:
        latest_processed = wallet_votes_collection.find_one(
            {},  # Empty filter to match all documents
            sort=[("block_height", pymongo.DESCENDING)]  # Sort by block height descending
        )
        
        if latest_processed:
            last_processed_height = int(latest_processed.get('block_height', 0))
        else:
            last_processed_height = 0
            
    except Exception as e:
        st.error(f"Error getting last processed height: {str(e)}")
        last_processed_height = 0

    endpoint = await find_best_rpc()
    if not endpoint:
        raise Exception("No working RPC endpoints found")

    transactions = []
    page = 1
    per_page = 25
    should_continue = True
    
    try:
        async with websockets.connect(endpoint, ping_interval=None) as websocket:
            # Build query for try_place_vote transactions
            query_str = f"wasm._contract_address='{CONTRACT_ADDRESS}' AND wasm.action='try_place_vote'"
            
            # Get total count first
            initial_query = {
                "jsonrpc": "2.0",
                "id": 0,
                "method": "tx_search",
                "params": {
                    "query": query_str,
                    "prove": False,
                    "page": "1",
                    "per_page": "1",
                    "order_by": "desc"
                }
            }
            
            await websocket.send(json.dumps(initial_query))
            initial_response = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            initial_data = json.loads(initial_response)
            
            if "result" not in initial_data:
                st.error(f"Error getting total count: {initial_data}")
                return []
                
            total_count = int(initial_data["result"].get("total_count", 0))
            
            if total_count == 0:
                return []
            
            total_pages = (total_count + per_page - 1) // per_page
            
            while page <= total_pages and should_continue:
                try:
                    query = {
                        "jsonrpc": "2.0",
                        "id": page,
                        "method": "tx_search",
                        "params": {
                            "query": query_str,
                            "prove": False,
                            "page": str(page),
                            "per_page": str(per_page),
                            "order_by": "desc"
                        }
                    }
                    
                    await websocket.send(json.dumps(query))
                    response = await asyncio.wait_for(websocket.recv(), timeout=15.0)
                    data = json.loads(response)
                    
                    if "result" not in data:
                        st.error(f"Error in response: {data}")
                        break
                        
                    txs = data["result"].get("txs", [])
                    
                    if not txs:
                        break
                    
                    # Process transactions until we hit one we've already seen
                    for tx in txs:
                        block_height = int(tx["height"])
                        
                        # If we've hit a block we've already processed, we can stop
                        if block_height <= last_processed_height:
                            should_continue = False
                            break
                            
                        try:
                            # Extract basic transaction info
                            tx_data = {
                                "transaction_hash": tx["hash"],
                                "block_height": int(tx["height"]),
                                "block_timestamp": tx.get("timestamp") or tx.get("header", {}).get("timestamp"),
                            }
                            
                            # Get raw transaction data
                            raw_tx = tx.get("tx")
                            if not raw_tx:
                                continue
                            
                            # Decode base64 tx
                            decoded_tx = base64.b64decode(raw_tx)
                            
                            # Look for the place_vote JSON string in the decoded data
                            try:
                                # Convert to string but keep raw bytes for searching
                                decoded_str = decoded_tx.decode('latin1')  # Use latin1 to preserve bytes
                                
                                # Find the place_vote JSON object
                                vote_marker = '{"place_vote":'
                                vote_start = decoded_str.find(vote_marker)
                                
                                if vote_start != -1:
                                    # Find the start and end of the JSON object
                                    json_start = vote_start
                                    brace_count = 0
                                    pos = json_start
                                    
                                    while pos < len(decoded_str):
                                        if decoded_str[pos] == '{':
                                            brace_count += 1
                                        elif decoded_str[pos] == '}':
                                            brace_count -= 1
                                            if brace_count == 0:
                                                json_end = pos + 1
                                                break
                                        pos += 1
                                    
                                    if brace_count == 0:
                                        # Extract the JSON string and parse it
                                        json_str = decoded_str[json_start:json_end]
                                        msg_data = json.loads(json_str)
                                        
                                        # Get sender from events
                                        sender = None
                                        for event in tx.get("tx_result", {}).get("events", []):
                                            if event.get("type") == "message":
                                                for attr in event.get("attributes", []):
                                                    if attr.get("key") == "sender":
                                                        sender = attr.get("value")
                                                        break
                                                if sender:
                                                    break
                                        
                                        if sender and "place_vote" in msg_data:
                                            tx_data["sender"] = sender
                                            tx_data["votes"] = [
                                                {
                                                    "lp_token": vote["lp_token"],
                                                    "weight": vote["weight"]
                                                } 
                                                for vote in msg_data["place_vote"]["weight_allocation"]
                                            ]
                                            
                                            transactions.append(tx_data)
                                            # st.success(f"Successfully processed transaction: {tx['hash']}")

                            except Exception as e:
                                st.error(f"Error parsing message for {tx['hash']}: {str(e)}")
                                continue

                        except Exception as e:
                            st.error(f"Error processing transaction {tx.get('hash')}: {str(e)}")
                            continue

                    progress = min(1.0, page / total_pages)
                    progress_bar.progress(progress, f"Processing page {page}/{total_pages}")
                    page += 1
                    
                    # Stop if we've hit previously processed blocks
                    if not should_continue:
                        break

                except asyncio.TimeoutError:
                    st.warning(f"Timeout on page {page}, retrying...")
                    await asyncio.sleep(1)
                    continue
                except Exception as e:
                    st.error(f"Error processing page {page}: {str(e)}")
                    await asyncio.sleep(1)
                    continue
                
    except Exception as e:
        st.error(f"Error in WebSocket connection: {str(e)}")
    
    return transactions

def store_transactions(transactions):
    """Store transactions in MongoDB"""
    if not transactions:
        return
        
    operations = []
    for tx in transactions:
        operations.append(
            UpdateOne(
                {"transaction_hash": tx["transaction_hash"]},  # Use transaction_hash as unique identifier
                {"$set": tx},
                upsert=True
            )
        )
    
    try:
        result = wallet_votes_collection.bulk_write(operations)
        # st.success(f"Successfully stored {len(transactions)} transactions")
    except BulkWriteError as e:
        st.error(f"Error storing transactions: {str(e)}")
        raise

def fetch_epochs():
    """Fetch epoch data from the contract"""
    try:
        # Find best REST endpoint (reusing logic from equinox_voting.py)
        response = requests.get("https://raw.githubusercontent.com/cosmos/chain-registry/master/neutron/chain.json")
        data = response.json()
        
        rest_endpoints = []
        for api in data.get("apis", {}).get("rest", []):
            address = api.get("address", "")
            if address.startswith("http"):
                rest_endpoints.append(address.rstrip('/'))
        
        # Add additional endpoints
        additional_endpoints = [
            "https://rest.neutron.nodestake.top",
            "https://neutron-rest.publicnode.com",
            "https://api-neutron.cosmos.nodestake.top"
        ]
        
        for endpoint in additional_endpoints:
            if endpoint not in rest_endpoints:
                rest_endpoints.append(endpoint)
        
        # Try endpoints until one works
        for endpoint in rest_endpoints:
            try:
                query = base64.b64encode(json.dumps({"voter_info": {}}).encode()).decode()
                url = f"{endpoint}/cosmwasm/wasm/v1/contract/{CONTRACT_ADDRESS}/smart/{query}"
                response = requests.get(url, timeout=2)
                if response.status_code == 200:
                    return response.json()['data']['vote_results']
            except:
                continue
        
        raise Exception("No working REST endpoints found")
    except Exception as e:
        st.error(f"Error fetching epochs: {str(e)}")
        return []

def fetch_pool_data():
    """Fetch pool names from Astroport API"""
    chains = [
        'neutron-1',
        'phoenix-1',
        'injective-1',
        'pacific-1',
        'osmosis-1'
    ]
    
    try:
        all_pools = []
        for chain in chains:
            url = f"https://app.astroport.fi/api/trpc/pools.getAll?input=%7B%22json%22%3A%7B%22chainId%22%3A%5B%22{chain}%22%5D%7D%7D"
            response = requests.get(url)
            if response.status_code == 200:
                chain_data = response.json()
                if chain_data and 'result' in chain_data and 'data' in chain_data['result'] and 'json' in chain_data['result']['data']:
                    all_pools.extend(chain_data['result']['data']['json'])
        
        # Create mapping of LP tokens to pool names
        pool_names = {}
        for pool in all_pools:
            pool_names[pool['lpAddress']] = pool['name']
        
        return pool_names
        
    except Exception as e:
        st.error(f"Error fetching pool data: {str(e)}")
        return {}

def get_wallet_essence(wallet_address: str) -> dict:
    """Fetch wallet essence data from contract"""
    # Get list of REST endpoints (similar to fetch_epochs)
    try:
        response = requests.get("https://raw.githubusercontent.com/cosmos/chain-registry/master/neutron/chain.json")
        data = response.json()
        
        rest_endpoints = []
        for api in data.get("apis", {}).get("rest", []):
            address = api.get("address", "")
            if address.startswith("http"):
                rest_endpoints.append(address.rstrip('/'))
        
        # Add additional endpoints
        additional_endpoints = [
            "https://rest.neutron.nodestake.top",
            "https://neutron-rest.publicnode.com",
            "https://api-neutron.cosmos.nodestake.top",
            "https://neutron-api.lavenderfive.com",
            "https://api.neutron.nodestake.top"
        ]
        
        for endpoint in additional_endpoints:
            if endpoint not in rest_endpoints:
                rest_endpoints.append(endpoint)
        
        # Try each endpoint until one works
        query = {
            "user": {
                "address": wallet_address
            }
        }
        query_b64 = base64.b64encode(json.dumps(query).encode()).decode()
        
        for endpoint in rest_endpoints:
            try:
                url = f"{endpoint}/cosmwasm/wasm/v1/contract/{CONTRACT_ADDRESS}/smart/{query_b64}"
                response = requests.get(url, timeout=5)
                if response.status_code == 200:
                    data = response.json()
                    if 'data' in data and len(data['data']) > 0:
                        return {
                            "wallet_address": wallet_address,
                            "essence_value": data['data'][0].get('essence_value', '0'),
                            "snapshot_time": datetime.utcnow()
                        }
            except Exception as e:
                continue  # Try next endpoint
        
        raise Exception("No working REST endpoints found")
        
    except Exception as e:
        st.error(f"Error fetching essence for {wallet_address}: {str(e)}")
        return None

def get_wallet_essences(wallets: List[str], progress_bar) -> Dict[str, str]:
    """Get essence values for all wallets"""
    # Check if we have a recent snapshot
    latest_snapshot = wallet_essence_collection.find_one(
        sort=[("snapshot_time", pymongo.DESCENDING)]
    )
    
    if latest_snapshot and datetime.utcnow() - latest_snapshot['snapshot_time'] < timedelta(hours=ESSENCE_SNAPSHOT_VALIDITY_HOURS):
        # Use existing snapshot
        snapshots = list(wallet_essence_collection.find())
        return {
            snap['wallet_address']: snap['essence_value'] 
            for snap in snapshots
        }, latest_snapshot['snapshot_time']
    
    # Need new snapshot
    progress_bar.progress(0, "Fetching wallet voting power...")
    essence_data = []
    
    for i, wallet in enumerate(wallets):
        essence = get_wallet_essence(wallet)
        if essence:
            essence_data.append(essence)
        progress = (i + 1) / len(wallets)
        progress_bar.progress(progress, f"Fetching voting power {i+1}/{len(wallets)}")
        time.sleep(0.1)  # Rate limiting
    
    # Store new snapshot
    if essence_data:
        wallet_essence_collection.delete_many({})  # Clear old snapshots
        wallet_essence_collection.insert_many(essence_data)
        snapshot_time = essence_data[0]['snapshot_time']
        return {
            data['wallet_address']: data['essence_value']
            for data in essence_data
        }, snapshot_time
    
    return {}, None

def create_dashboard():
    st.title("Equinox Wallet Voting Dashboard")
    
    # Initialize session state
    if 'syncing' not in st.session_state:
        st.session_state.syncing = False
    
    # Progress bar placeholder
    progress_bar = st.empty()
    
    # Automatic sync on first run
    if 'first_run' not in st.session_state:
        st.session_state.first_run = True
        st.session_state.syncing = True
        
        # Create progress bar
        progress_bar = st.progress(0, "Starting transaction sync...")
        
        try:
            # Fetch and store transactions
            transactions = asyncio.run(fetch_transactions(progress_bar))
            if transactions:
                store_transactions(transactions)
            
        except Exception as e:
            st.error(f"Error syncing transactions: {str(e)}")
        finally:
            st.session_state.syncing = False
            st.session_state.first_run = False
            progress_bar.empty()

    # Fetch epochs and pool names
    epochs = fetch_epochs()
    pool_names = fetch_pool_data()
    
    if not epochs:
        st.error("No epoch data available")
        return
    
    # Create epoch selector
    epoch_options = []
    # Sort epochs by epoch_id in descending order to get most recent first
    sorted_epochs = sorted(epochs, key=lambda x: x['epoch_id'], reverse=True)
    for epoch in sorted_epochs:
        end_date = datetime.fromtimestamp(epoch['end_date'])
        epoch_options.append(f"Epoch {epoch['epoch_id']} (ends {end_date.strftime('%Y-%m-%d')})")
    
    selected_epoch = st.selectbox("Select Epoch", epoch_options, index=0)
    epoch_id = int(selected_epoch.split()[1])
    epoch_data = next((e for e in epochs if e['epoch_id'] == epoch_id), None)
    
    if epoch_data:
        # Show loading spinner while aggregating data
        with st.spinner('Preparing voting data...'):
            end_timestamp = datetime.fromtimestamp(epoch_data['end_date'])
            
            pipeline = [
                {
                    "$match": {
                        "$or": [
                            {"block_timestamp": {"$lte": end_timestamp}},
                            {"block_timestamp": None}
                        ]
                    }
                },
                {
                    "$sort": {
                        "block_height": 1
                    }
                },
                {
                    "$group": {
                        "_id": "$sender",
                        "latest_vote": {"$last": "$$ROOT"}
                    }
                }
            ]
            
            wallet_votes = list(wallet_votes_collection.aggregate(pipeline))
        
        if wallet_votes:
            # Get wallet essences
            wallet_addresses = [vote['latest_vote']['sender'] for vote in wallet_votes]
            with st.spinner('Fetching wallet voting power...'):
                essence_values, snapshot_time = get_wallet_essences(wallet_addresses, st.progress(0))
            
            if snapshot_time:
                st.info(f"Voting weight as per {snapshot_time.strftime('%Y-%m-%d')}")
            
            st.subheader(f"Wallet Voting Distribution (Total Wallets: {len(wallet_votes)})")
            
            # Group and sort by essence value
            wallet_groups = {}
            for wallet_vote in wallet_votes:
                sender = wallet_vote['latest_vote']['sender']
                votes = wallet_vote['latest_vote']['votes']
                # Convert essence value to millions and round to integer
                essence = int(int(essence_values.get(sender, '0')) / 1_000_000)
                
                wallet_groups[sender] = {
                    'essence': essence,
                    'votes': sorted(votes, key=lambda x: float(x['weight']), reverse=True)
                }
            
            # Sort wallets by essence value
            sorted_wallets = sorted(
                wallet_groups.items(),
                key=lambda x: x[1]['essence'],
                reverse=True
            )
            
            # Display each wallet group
            for wallet, data in sorted_wallets:
                st.markdown(f"#### Wallet: `{wallet}`")
                st.markdown(f"*Cosmic Essence: {data['essence']:,}*")  # Already in millions and formatted with commas
                
                # Create DataFrame for this wallet's votes
                df = pd.DataFrame([
                    {
                        "Pool": pool_names.get(vote['lp_token'], vote['lp_token']),
                        "Vote Weight (%)": f"{float(vote['weight']) * 100:.2f}%",
                        "Visual Distribution": float(vote['weight']) * 100
                    }
                    for vote in data['votes']
                ])
                
                # Format the progress bars
                df['Visual Distribution'] = df['Visual Distribution'].apply(
                    lambda pct: f"""
                        <div style="width:100%; height:20px; background-color:#f0f2f6; border-radius:10px; overflow:hidden">
                            <div style="width:{pct}%; height:100%; background-color:#00cc00; border-radius:10px"></div>
                        </div>
                    """
                )
                
                # Add CSS for table formatting
                st.markdown("""
                    <style>
                        table {
                            width: 100% !important;
                            table-layout: fixed !important;
                        }
                        th, td {
                            text-align: left !important;
                            width: 33.33% !important;
                            word-wrap: break-word !important;
                        }
                    </style>
                """, unsafe_allow_html=True)
                
                # Display the table
                st.write(
                    df.to_html(
                        escape=False,
                        index=False,
                        formatters={'Visual Distribution': lambda x: x}
                    ),
                    unsafe_allow_html=True
                )
        else:
            st.info("No voting data available for this epoch")

if __name__ == "__main__":
    create_dashboard()
