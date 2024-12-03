import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime
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
    
    return db[COLLECTION_NAME]

# Initialize MongoDB collection
wallet_votes_collection = setup_mongodb()

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
                                            st.success(f"Successfully processed transaction: {tx['hash']}")

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
        st.success(f"Successfully stored {len(transactions)} transactions")
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
    for epoch in epochs:
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
            st.subheader(f"Wallet Voting Distribution (Total Wallets: {len(wallet_votes)})")
            
            # Show loading spinner while formatting data
            with st.spinner('Formatting vote distribution...'):
                # Group votes by wallet first
                wallet_groups = {}
                for wallet_vote in wallet_votes:
                    sender = wallet_vote['latest_vote']['sender']
                    votes = wallet_vote['latest_vote']['votes']
                    
                    # Sort votes by weight descending
                    sorted_votes = sorted(votes, key=lambda x: float(x['weight']), reverse=True)
                    
                    wallet_groups[sender] = [
                        {
                            "Pool": pool_names.get(vote['lp_token'], vote['lp_token']),
                            "Vote Weight (%)": f"{float(vote['weight']) * 100:.2f}%",
                            "Visual Distribution": float(vote['weight']) * 100
                        }
                        for vote in sorted_votes
                    ]
                
                # Display each wallet group
                for wallet, votes in wallet_groups.items():
                    st.markdown(f"#### Wallet: `{wallet}`")
                    
                    # Create DataFrame for this wallet's votes
                    df = pd.DataFrame(votes)
                    
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
