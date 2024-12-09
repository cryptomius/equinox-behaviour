import streamlit as st
import requests
import json
from datetime import datetime
from dateutil.parser import parse
import base64
import asyncio
import nest_asyncio
import pandas as pd
import plotly.express as px

# Enable nested event loops
nest_asyncio.apply()

# Set page config
st.set_page_config(
    page_title="Equinox Voting Behavior",
    page_icon="📊",
    layout="wide"
)

# Add title
st.title("Equinox Voting Behavior Dashboard")

async def get_rpc_endpoints():
    """Fetch RPC endpoints from Cosmos Chain Registry"""
    try:
        response = requests.get("https://raw.githubusercontent.com/cosmos/chain-registry/master/neutron/chain.json")
        data = response.json()
        
        # Get REST endpoints
        rest_endpoints = []
        for api in data.get("apis", {}).get("rest", []):
            address = api.get("address", "")
            if address.startswith("http"):
                rest_endpoints.append(address.rstrip('/'))
        
        # Add additional known endpoints
        additional_endpoints = [
            "https://rest.neutron.nodestake.top",
            "https://neutron-rest.publicnode.com",
            "https://api-neutron.cosmos.nodestake.top"
        ]
        
        # Add any endpoints that aren't already in the list
        for endpoint in additional_endpoints:
            if endpoint not in rest_endpoints:
                rest_endpoints.append(endpoint)
        
        return rest_endpoints
    except Exception as e:
        st.error(f"Error fetching RPC endpoints: {str(e)}")
        return []

async def test_rpc_endpoint(endpoint):
    """Test if an RPC endpoint is responsive"""
    try:
        contract_address = "neutron13l5nw6fh4xascjfyru5pe9w5ur8rp7svzl7hhkdh7jfdk3fup75quz9zy5"
        query = base64.b64encode(json.dumps({"voter_info": {}}).encode()).decode()
        
        url = f"{endpoint}/cosmwasm/wasm/v1/contract/{contract_address}/smart/{query}"
        response = requests.get(url, timeout=2)
        
        if response.status_code == 200:
            return endpoint, True
        return endpoint, False
    except:
        return endpoint, False

async def find_best_rpc():
    """Find the best working RPC endpoint"""
    endpoints = await get_rpc_endpoints()
    working_endpoints = []
    
    # Create a progress bar
    progress_text = "Testing RPC endpoints..."
    progress_bar = st.progress(0, text=progress_text)
    
    # Test all endpoints
    for i, endpoint in enumerate(endpoints):
        progress_bar.progress((i + 1) / len(endpoints), text=f"Testing {endpoint}...")
        endpoint, success = await test_rpc_endpoint(endpoint)
        if success:
            working_endpoints.append(endpoint)
    
    # Clean up progress bar
    progress_bar.empty()
    
    return working_endpoints[0] if working_endpoints else None

# Function to fetch voting data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_voting_data():
    contract_address = "neutron13l5nw6fh4xascjfyru5pe9w5ur8rp7svzl7hhkdh7jfdk3fup75quz9zy5"
    query = {
        "voter_info": {}
    }
    
    try:
        with st.spinner('Finding best RPC endpoint...'):
            # Find best RPC endpoint
            endpoint = asyncio.run(find_best_rpc())
            if not endpoint:
                raise Exception("No working RPC endpoints found")
        
        with st.spinner('Fetching voting data...'):
            # Convert query to JSON and encode it
            encoded_query = base64.b64encode(json.dumps(query).encode()).decode()
            url = f"{endpoint}/cosmwasm/wasm/v1/contract/{contract_address}/smart/{encoded_query}"
            response = requests.get(url)
            return response.json()
    except Exception as e:
        st.error(f"Error fetching voting data: {str(e)}")
        return None

# Function to fetch pool data
@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_pool_data():
    chains = [
        'neutron-1',
        'phoenix-1',
        'injective-1',
        'pacific-1',
        'osmosis-1'
    ]
    
    try:
        all_pools = []
        
        # Create a progress bar
        progress_text = "Fetching pool data..."
        progress_bar = st.progress(0, text=progress_text)
        
        for i, chain in enumerate(chains):
            progress_bar.progress((i + 1) / len(chains), text=f"Fetching {chain} pools...")
            url = f"https://app.astroport.fi/api/trpc/pools.getAll?input=%7B%22json%22%3A%7B%22chainId%22%3A%5B%22{chain}%22%5D%7D%7D"
            response = requests.get(url)
            if response.status_code == 200:
                chain_data = response.json()
                if chain_data and 'result' in chain_data and 'data' in chain_data['result'] and 'json' in chain_data['result']['data']:
                    all_pools.extend(chain_data['result']['data']['json'])
        
        # Clean up progress bar
        progress_bar.empty()
        
        return {'result': {'data': {'json': all_pools}}} if all_pools else None
    except Exception as e:
        st.error(f"Error fetching pool data: {str(e)}")
        return None

# Get data
voting_data = fetch_voting_data()
pool_data = fetch_pool_data()

if voting_data and pool_data:
    # Create mapping of LP tokens to pool names
    pool_names = {}
    for pool in pool_data['result']['data']['json']:
        pool_names[pool['lpAddress']] = pool['name']

    # Get list of epochs with their end dates
    epochs = []
    for result in voting_data['data']['vote_results']:
        epoch_id = result['epoch_id']
        end_date = datetime.fromtimestamp(result['end_date']).strftime('%Y-%m-%d')
        epochs.append(f"Epoch {epoch_id} (ends {end_date})")

    # Create selectbox for epochs, defaulting to the most recent epoch
    selected_epoch = st.selectbox("Select Epoch", epochs, index=len(epochs)-1)
    
    # Get selected epoch data
    epoch_id = int(selected_epoch.split()[1])
    epoch_data = next(
        (result for result in voting_data['data']['vote_results'] if result['epoch_id'] == epoch_id),
        None
    )

    if epoch_data:

        # Create table of pool votes
        st.subheader("Pool Vote Distribution")
        
        # Get previous epoch data if available
        prev_epoch_data = None
        if epoch_id > 1:
            prev_epoch_data = next(
                (result for result in voting_data['data']['vote_results'] if result['epoch_id'] == epoch_id - 1),
                None
            )
        
        # Create mapping of previous epoch weights if available
        prev_weights = {}
        if prev_epoch_data:
            for weight in prev_epoch_data['elector_weights']:
                prev_weights[weight['lp_token']] = float(weight['weight']) * 100
        
        # Prepare data for the table
        table_data = []
        for weight in epoch_data['elector_weights']:
            pool_name = pool_names.get(weight['lp_token'], weight['lp_token'])
            vote_weight = float(weight['weight']) * 100
            
            # Calculate delta if previous epoch data exists
            delta = None
            if prev_weights:
                prev_weight = prev_weights.get(weight['lp_token'], 0)
                delta = vote_weight - prev_weight
            
            row_data = {
                "Pool": pool_name,
                "Vote Weight (%)": f"{vote_weight:.1f}%",
                "Visual Distribution": vote_weight  # Raw percentage for sorting
            }
            
            # Add delta column if we have previous data and change is >= 1%
            if delta is not None and abs(delta) >= 1.0:
                # Create delta display with color and arrow
                arrow = "▲" if delta > 0 else "▼" if delta < 0 else "−"
                color = "#00cc00" if delta > 0 else "#ff4b4b" if delta < 0 else "#666666"
                delta_display = f'<span style="color: {color}">{arrow} {abs(delta):.1f}%</span>'
                row_data["Δ from Previous"] = delta_display
            elif delta is not None:
                row_data["Δ from Previous"] = ""  # Empty string for changes < 1%
            
            table_data.append(row_data)
        
        # Convert to DataFrame and sort
        df = pd.DataFrame(table_data)
        df = df.sort_values('Visual Distribution', ascending=False)
        
        # Format the progress bars
        def make_progress_bar(pct):
            return f"""
                <div style="width:100%; height:20px; background-color:#f0f2f6; border-radius:10px; overflow:hidden">
                    <div style="width:{pct}%; height:100%; background-color:#00cc00; border-radius:10px"></div>
                </div>
            """
        
        # Apply formatting
        df['Visual Distribution'] = df['Visual Distribution'].apply(make_progress_bar)
        
        # Display the table with HTML
        st.write(
            df.to_html(
                escape=False,
                index=False,
                formatters={'Visual Distribution': lambda x: x}
            ),
            unsafe_allow_html=True
        )
        
        # Add some CSS to make the table look better
        st.markdown("""
            <style>
                table {
                    width: 100%;
                    border-collapse: collapse;
                }
                th {
                    background-color: #f0f2f6;
                    padding: 12px;
                    text-align: left;
                }
                td {
                    padding: 12px;
                    border-bottom: 1px solid #f0f2f6;
                }
                tr:hover {
                    background-color: #f8f9fa;
                }
            </style>
        """, unsafe_allow_html=True)

        # Create historical voting trend chart
        st.subheader("Historical Voting Trends")
        
        # Process all epochs data
        trend_data = []
        for result in voting_data['data']['vote_results']:
            epoch_id = result['epoch_id']
            end_date = datetime.fromtimestamp(result['end_date']).strftime('%Y-%m-%d')
            
            for weight in result['elector_weights']:
                pool_name = pool_names.get(weight['lp_token'], weight['lp_token'])
                vote_weight = float(weight['weight']) * 100
                trend_data.append({
                    'Epoch': f"Epoch {epoch_id}",
                    'End Date': end_date,
                    'Pool': pool_name,
                    'Vote Weight (%)': vote_weight
                })
        
        # Convert to DataFrame
        trend_df = pd.DataFrame(trend_data)
        
        # Calculate average vote weight per pool across all epochs
        avg_votes = trend_df.groupby('Pool')['Vote Weight (%)'].mean().sort_values(ascending=False)
        
        # Filter pools with less than 1% average votes and create a new DataFrame
        significant_pools = avg_votes[avg_votes >= 1.0].index
        trend_df_filtered = trend_df[trend_df['Pool'].isin(significant_pools)].copy()  # Create explicit copy
        
        # Sort pools by their average vote weight for consistent colors
        pool_order = avg_votes[avg_votes >= 1.0].index.tolist()
        trend_df_filtered.loc[:, 'Pool'] = pd.Categorical(trend_df_filtered['Pool'], categories=pool_order, ordered=True)
        
        # Create line chart
        fig = px.line(
            trend_df_filtered,
            x='Epoch',
            y='Vote Weight (%)',
            color='Pool',
            markers=True,
            title='Voting Trends Across Epochs (Pools with >1% Average Votes)',
            category_orders={'Pool': pool_order}  # This ensures consistent color assignment
        )
        
        # Update layout
        fig.update_layout(
            xaxis_title="Epoch",
            yaxis_title="Vote Weight (%)",
            legend_title="Pool",
            hovermode='x unified',
            legend=dict(
                traceorder='normal',  # This ensures the legend follows our category order
                itemsizing='constant'
            )
        )
        
        # Sort hover template to show pools in descending order of vote weight
        fig.update_traces(
            hovertemplate='%{y:.1f}%'  # Simplified hover text
        )
        
        # Custom hover data sorting
        fig.update_layout(
            hoverlabel=dict(
                font_size=12,
                font_family="Arial"
            ),
            # This function will sort the hover data
            hovermode='x unified',
            hoverdistance=100
        )
        
        # Display the chart
        st.plotly_chart(fig, use_container_width=True)
else:
    st.error("Failed to fetch data. Please check your internet connection and try again.")
