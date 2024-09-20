from logger_config import setup_logger
import asyncio
import aiohttp
import base64
import decimal
import hashlib
import ipaddress
import json
import os
import math
import platform
import statistics
import time
import warnings
from datetime import datetime, timedelta
from typing import Optional
import dirtyjson
import numpy as np
import pandas as pd
import psutil
from httpx import AsyncClient, Limits, Timeout
from dotenv import load_dotenv

try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse
logger = setup_logger()

block_data_cache = {}
number_of_cpus = os.cpu_count()
my_os = platform.system()
loop = asyncio.get_event_loop()
warnings.filterwarnings('ignore')
parent = psutil.Process()
if 'Windows' in my_os:
    parent.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)
else:
    parent.nice(19)

# Global variable to store recent block hashes
recent_block_hashes = {}
# Cache for storing blockchain sync status
sync_status_cache = {
    'last_updated': datetime.min,
    'is_fully_synced': True,
    'reason': 'Initial sync status'
}

async def update_sync_status_cache(rpc_connection):
    global sync_status_cache
    use_disable_sync_check = 1
    while True:
        try:
            if use_disable_sync_check:
                is_synced = True
                reason = 'Sync check disabled'
            else:
                is_synced, reason = await check_if_blockchain_is_fully_synced()
            sync_status_cache = {
                'last_updated': datetime.now(),
                'is_fully_synced': is_synced,
                'reason': reason
            }
        except Exception as e:
            logger.error(f"Error updating sync status: {e}")
        await asyncio.sleep(60)  # Update every 60 seconds
                
def is_blockchain_fully_synced():
    global sync_status_cache
    return sync_status_cache['is_fully_synced'], sync_status_cache['reason']
                
class ClientSessionManager:
    def __init__(self, stale_timeout: timedelta = timedelta(minutes=15)):
        self.client_session: Optional[aiohttp.ClientSession] = None
        self.last_used: datetime = datetime.min
        self.stale_timeout = stale_timeout

    async def is_valid_session(self, session: aiohttp.ClientSession) -> bool:
        try:
            async with session.get('http://microsoft.com', timeout=5) as response:
                return response.status == 200
        except Exception:
            return False

    async def get_or_create_session(self) -> aiohttp.ClientSession:
        now = datetime.utcnow()
        if self.client_session and (now - self.last_used < self.stale_timeout) and await self.is_valid_session(self.client_session):
            self.last_used = now
            return self.client_session
        await self.close_session()  # Close the existing invalid or stale session, if any
        self.last_used = now
        return await self.create_session()

    async def create_session(self) -> aiohttp.ClientSession:
        connector = aiohttp.TCPConnector(limit=10000)
        self.client_session = aiohttp.ClientSession(connector=connector)
        return self.client_session

    async def close_session(self):
        if self.client_session:
            await self.client_session.close()
            self.client_session = None

session_manager = ClientSessionManager() # Initialize global session manager

def parse_mime_type(mime_type):
    return tuple(mime_type.split(';')[0].split('/'))

DEFAULT_RPC_PORTS = {
    'mainnet': '9932',
    'testnet': '19932',
    'devnet': '29932',
    'regtest': '18232'
}

def get_local_rpc_settings_func(directory_with_pastel_conf=os.path.expanduser("~/.pastel/")):
    with open(os.path.join(directory_with_pastel_conf, "pastel.conf"), 'r') as f:
        lines = f.readlines()
    # Initialize variables
    other_flags = {}
    rpchost = '127.0.0.1'
    rpcport = '19932'
    rpcport_option = None
    rpcuser = 'default_user'
    rpcpassword = 'default_password'
    genpassphrase = None  # Initialize the genpassphrase variable
    network_mode = None
    
    for line in lines:
        if '=' in line:
            key, value = line.strip().split('=', 1)
            key = key.strip()
            value = value.strip()
            if key == 'rpcport':
                rpcport_option = value
            elif key == 'rpcuser':
                rpcuser = value
            elif key == 'rpcpassword':
                rpcpassword = value
            elif key == 'rpchost':
                rpchost = value
            elif key == 'genpassphrase':  # Check for genpassphrase
                genpassphrase = value
            else:
                other_flags[key] = value
                if key in DEFAULT_RPC_PORTS and value == '1':
                    network_mode = key

    if network_mode is not None:
        if rpcport_option is not None:
            rpcport = rpcport_option
        else:
            rpcport = DEFAULT_RPC_PORTS[network_mode]

    # Return all extracted values including genpassphrase
    return rpchost, rpcport, rpcuser, rpcpassword, genpassphrase, other_flags

def get_remote_rpc_settings_func():
    rpchost = '45.67.221.205'
    #rpchost = '209.145.54.164'
    rpcuser = 'IzfUzMZI'
    rpcpassword = 'ku5YhVtKSNWMIYp'
    rpcport = '9932'
    return rpchost, rpcport, rpcuser, rpcpassword    

class JSONRPCException(Exception):
    def __init__(self, rpc_error):
        parent_args = []
        try:
            parent_args.append(rpc_error['message'])
        except Exception as e:
            logger.error(f"Error occurred in JSONRPCException: {e}")
            pass
        Exception.__init__(self, *parent_args)
        self.error = rpc_error
        self.code = rpc_error['code'] if 'code' in rpc_error else None
        self.message = rpc_error['message'] if 'message' in rpc_error else None

    def __str__(self):
        return '%d: %s' % (self.code, self.message)

    def __repr__(self):
        return '<%s \'%s\'>' % (self.__class__.__name__, self)

def EncodeDecimal(o):
    if isinstance(o, decimal.Decimal):
        return float(o)
    elif isinstance(o, np.integer):
        return int(o)  # Convert numpy integers to Python int
    elif isinstance(o, np.floating):
        return float(o)  # Convert numpy floats to Python float
    elif isinstance(o, np.ndarray):
        return o.tolist()  # Convert numpy arrays to list
    elif isinstance(o, (int, float, str, bool, list, dict, type(None))):
        return o  # These types are already JSON serializable
    else:
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")
    
class AsyncAuthServiceProxy:
    max_concurrent_requests = 5000
    _semaphore = asyncio.BoundedSemaphore(max_concurrent_requests)
    def __init__(self, service_url, service_name=None, reconnect_timeout=15, reconnect_amount=2, request_timeout=20):
        self.service_url = service_url
        self.service_name = service_name
        self.url = urlparse.urlparse(service_url)        
        self.client = AsyncClient(timeout=Timeout(request_timeout), limits=Limits(max_connections=200, max_keepalive_connections=10))
        self.id_count = 0
        user = self.url.username
        password = self.url.password
        authpair = f"{user}:{password}".encode('utf-8')
        self.auth_header = b'Basic ' + base64.b64encode(authpair)
        self.reconnect_timeout = reconnect_timeout
        self.reconnect_amount = reconnect_amount
        self.request_timeout = request_timeout

    def __getattr__(self, name):
        if name.startswith('__') and name.endswith('__'):
            raise AttributeError
        if self.service_name is not None:
            name = f"{self.service_name}.{name}"
        return AsyncAuthServiceProxy(self.service_url, name)

    async def __call__(self, *args):
        async with self._semaphore: # Acquire a semaphore
            self.id_count += 1
            postdata = json.dumps({
                'version': '1.1',
                'method': self.service_name,
                'params': args,
                'id': self.id_count
            }, default=EncodeDecimal)
            headers = {
                'Host': self.url.hostname,
                'User-Agent': "AuthServiceProxy/0.1",
                'Authorization': self.auth_header,
                'Content-type': 'application/json'
            }
            for i in range(self.reconnect_amount):
                try:
                    if i > 0:
                        logger.warning(f"Reconnect try #{i+1}")
                        sleep_time = self.reconnect_timeout * (2 ** i)
                        logger.info(f"Waiting for {sleep_time} seconds before retrying.")
                        await asyncio.sleep(sleep_time)
                    response = await self.client.post(
                        self.service_url, headers=headers, data=postdata)
                    break
                except Exception as e:
                    logger.error(f"Error occurred in __call__: {e}")
                    err_msg = f"Failed to connect to {self.url.hostname}:{self.url.port}"
                    rtm = self.reconnect_timeout
                    if rtm:
                        err_msg += f". Waiting {rtm} seconds."
                    logger.exception(err_msg)
            else:
                logger.error("Reconnect tries exceeded.")
                return
            response_json = response.json()
            if response_json['error'] is not None:
                raise JSONRPCException(response_json['error'])
            elif 'result' not in response_json:
                raise JSONRPCException({
                    'code': -343, 'message': 'missing JSON-RPC result'})
            else:
                return response_json['result']

async def check_network_quality(rpc_connection):
    peer_info = await rpc_connection.getpeerinfo()
    latency_list = [peer['pingtime'] for peer in peer_info if 'pingtime' in peer]
    if not latency_list:
        return False
    average_latency = sum(latency_list) / len(latency_list)
    # A good threshold for latency might be 5 second
    return average_latency < 5.0

async def check_peers_quality(rpc_connection):
    peer_info = await rpc_connection.getpeerinfo()
    # Check if peers have the same block number for headers and blocks
    synced_peers = [peer for peer in peer_info if peer.get('synced_headers', -1) == peer.get('synced_blocks', -1)]
    # Assuming having at least 1 fully synced peers is a good sign
    return len(synced_peers) >= 1

async def check_timestamp_anomalies(rpc_connection):
    latest_block_hash = await rpc_connection.getbestblockhash()
    latest_block_header = await rpc_connection.getblockheader(latest_block_hash)
    latest_block_time = latest_block_header['time']
    # Allow a 10-minute window for timestamp discrepancies
    return abs(time.time() - latest_block_time) < 600  # 10 minutes in seconds

async def check_pending_transactions_pool(rpc_connection):
    mempool_info = await rpc_connection.getmempoolinfo()
    # Assuming a mempool size of 1000 transactions is reasonable
    return mempool_info['size'] < 1000


async def currently_in_initial_sync_period(rpc_connection) -> bool:
    try:
        expected_block_time_in_seconds = 2.5 * 60  # 2.5 minutes in seconds
        number_of_blocks_to_check = 20
        timestamps = []
        current_block_height: int = await rpc_connection.getblockcount() # type: ignore
        block_hashes = await rpc_connection.getblockhash(current_block_height - number_of_blocks_to_check, -1);
        for block in block_hashes: # type: ignore
            block_header = await rpc_connection.getblockheader(block['hash'])
            timestamps.append(block_header['time']) # type: ignore
        intervals = [timestamps[i] - timestamps[i - 1] for i in range(1, len(timestamps))]
        mean_interval = sum(intervals) / len(intervals)
        return mean_interval < expected_block_time_in_seconds / 2
    except Exception as e:
        logger.error(f"Error in currently_in_initial_sync_period: {e}")
        return False


async def update_recent_block_hashes(rpc_connection, number_of_blocks=20):
    global recent_block_hashes
    try:
        current_block_height = await rpc_connection.getblockcount()
        block_hashes = await rpc_connection.getblockhash(current_block_height - number_of_blocks, number_of_blocks)
        recent_block_hashes = { block['height']: block['hash'] for block in block_hashes }
    except Exception as e:
        logger.error(f"Error in update_recent_block_hashes: {e}")


async def check_for_chain_reorg(rpc_connection):
    global recent_block_hashes
    try:
        reorg_detected = False
        reorg_depth = 0
        if len(recent_block_hashes) < 20: # Check if we have enough data for the reorg check
            logger.info("Not enough data for reorg check, skipping.")
            return reorg_detected, reorg_depth
        # find min height in recent_block_hashes
        min_height = min(recent_block_hashes.keys())
        block_hashes = await rpc_connection.getblockhash(min_height, -1)
        if not isinstance(block_hashes, list) or len(block_hashes) == 0:
            logger.error("getblockhash returned an empty or invalid response")
            return False, 0
        
        new_block_hashes = { block['height']: block['hash'] for block in block_hashes }
        # Iterate over the recent block hashes and compare with the current chain
        for height, stored_hash in recent_block_hashes.items():
            current_hash = new_block_hashes.get(height)
            if current_hash != stored_hash:
                reorg_detected = True
                reorg_depth = max(reorg_depth, len(recent_block_hashes) - (height - min(recent_block_hashes.keys())))
                break
        return reorg_detected, reorg_depth
    except Exception as e:
        logger.error(f"Error in check_for_chain_reorg: {e}")
        return False, 0
    
async def periodic_update_task(rpc_connection, update_interval=60):
    while True:
        await update_recent_block_hashes(rpc_connection)
        await asyncio.sleep(update_interval)
    
async def check_if_blockchain_is_fully_synced(rpc_connection, use_optional_checks = 0) -> (bool, str):
    expected_block_time_in_seconds = 2.5 * 60
    number_of_past_minutes_to_check_for_new_block = 15
    reason_for_thinking_we_are_not_fully_synced = ''
    try:
        blockchain_info = await rpc_connection.getblockchaininfo()
        # Check the verification progress to determine if the node is almost or fully synced
        if blockchain_info['verificationprogress'] < 0.999:  # Adjust this threshold as needed
            reason_for_thinking_we_are_not_fully_synced += 'Blockchain verification in progress. '
        # Check if Pasteld is in the initial sync period
        if await currently_in_initial_sync_period(rpc_connection):
            reason_for_thinking_we_are_not_fully_synced += 'Pasteld is in initial sync period. '
        # Check if there are new blocks in a reasonable time; Get the latest block's timestamp and compare it with the current system time:
        latest_block_hash = blockchain_info['bestblockhash']
        latest_block_header = await rpc_connection.getblockheader(latest_block_hash)
        latest_block_time = latest_block_header['time']
        if time.time() - latest_block_time > (expected_block_time_in_seconds * number_of_past_minutes_to_check_for_new_block):
            reason_for_thinking_we_are_not_fully_synced += 'No new blocks for a long period. '
        # Check for peer connections
        network_info = await rpc_connection.getnetworkinfo()
        if not network_info['connections']:
            reason_for_thinking_we_are_not_fully_synced += 'No peer connections. '
        # Check for large chain reorg
        reorg_detected, reorg_depth = await check_for_chain_reorg(rpc_connection)
        if reorg_detected:
            reason_for_thinking_we_are_not_fully_synced += f'Chain reorganization detected. Depth: {reorg_depth}. '
        if use_optional_checks:
            # Check network quality based on the average latency of peer connections.  A high average latency indicates potential network issues that could affect synchronization.
            if not await check_network_quality(rpc_connection):
                reason_for_thinking_we_are_not_fully_synced += 'Network quality is poor. '
            # Check the quality of peer connections. This ensures there are enough peers that are fully synced. A low number of fully synced peers can hinder proper synchronization with the blockchain network.
            if not await check_peers_quality(rpc_connection):
                reason_for_thinking_we_are_not_fully_synced += 'Insufficient quality of peer connections. '
            # Check for anomalies in the timestamp of the latest block. Significant deviations between the block timestamp and the system time can indicate potential issues with the node's time synchronization.
            if not await check_timestamp_anomalies(rpc_connection):
                reason_for_thinking_we_are_not_fully_synced += 'Timestamp anomalies detected. '
            # Check the size of the pending transactions pool (mempool). An excessively large mempool size might indicate network congestion or issues in processing transactions.
            if not await check_pending_transactions_pool(rpc_connection):
                reason_for_thinking_we_are_not_fully_synced += 'Pending transactions pool is too large. '
        # Determine if fully synced
        fully_synced = len(reason_for_thinking_we_are_not_fully_synced) == 0
        return fully_synced, reason_for_thinking_we_are_not_fully_synced.strip()
    except Exception as e:
        logger.error(f"Error in check_if_blockchain_is_fully_synced: {e}")
        return False, f"Error encountered: {e}"


async def get_current_pastel_block_height_func(rpc_connection):
    fully_synced, reason_for_thinking_we_are_not_fully_synced = is_blockchain_fully_synced()
    if not fully_synced:
        logger.error(f"Blockchain is not fully synced! Reason: {reason_for_thinking_we_are_not_fully_synced}")
        return reason_for_thinking_we_are_not_fully_synced
    
    return await rpc_connection.getblockcount()

async def get_best_block_hash_and_merkle_root_func(rpc_connection):
    fully_synced, reason_for_thinking_we_are_not_fully_synced = is_blockchain_fully_synced()
    if not fully_synced:
        logger.error(f"Blockchain is not fully synced! Reason: {reason_for_thinking_we_are_not_fully_synced}")
        return reason_for_thinking_we_are_not_fully_synced, reason_for_thinking_we_are_not_fully_synced, reason_for_thinking_we_are_not_fully_synced
    else:
        best_block_height = await get_current_pastel_block_height_func(rpc_connection)
        best_block_details = await rpc_connection.getblock(best_block_height)
        best_block_hash = best_block_details['hash']
        best_block_merkle_root = best_block_details['merkleroot']
        return best_block_hash, best_block_merkle_root, best_block_height

async def get_last_block_data_func(rpc_connection):
    fully_synced, reason_for_thinking_we_are_not_fully_synced = is_blockchain_fully_synced()
    if not fully_synced:
        logger.error(f"Blockchain is not fully synced! Reason: {reason_for_thinking_we_are_not_fully_synced}")
        return reason_for_thinking_we_are_not_fully_synced
    else:      
        current_block_height = await get_current_pastel_block_height_func(rpc_connection)
        block_data = await rpc_connection.getblock(str(current_block_height))
        return block_data


async def check_psl_address_balance_func(rpc_connection, address_to_check):
    balance_at_address = await rpc_connection.z_getbalance(address_to_check)
    return balance_at_address


async def get_raw_transaction_func(rpc_connection, txid):
    raw_transaction_data = await rpc_connection.getrawtransaction(txid, 1) 
    return raw_transaction_data


async def sign_message_with_pastelid_func(rpc_connection, pastelid, message_to_sign, passphrase) -> str:
    results_dict = await rpc_connection.pastelid('sign', message_to_sign, pastelid, passphrase, 'ed448')
    return results_dict['signature']


async def sign_base64_encoded_message_with_pastelid_func(rpc_connection, pastelid, base64_message_to_sign, passphrase) -> str:
    results_dict = await rpc_connection.pastelid('sign-base64-encoded', base64_message_to_sign, pastelid, passphrase, 'ed448')
    return results_dict['signature']

async def verify_message_with_pastelid_func(rpc_connection, pastelid, message_to_verify, pastelid_signature_on_message) -> str:
    verification_result = await rpc_connection.pastelid('verify', message_to_verify, pastelid_signature_on_message, pastelid, 'ed448')
    return verification_result['verification']


async def verify_base64_encoded_message_with_pastelid_func(rpc_connection, pastelid, message_to_verify, pastelid_signature_on_message) -> str:
    verification_result = await rpc_connection.pastelid('verify-base64-encoded', message_to_verify, pastelid_signature_on_message, pastelid, 'ed448')
    return verification_result['verification']

async def check_masternode_top_func(rpc_connection):
    masternode_top_command_output = await rpc_connection.masternode('top')
    return masternode_top_command_output

async def check_supernode_list_func(rpc_connection):
    masternode_list_full_command_output = await rpc_connection.masternodelist('full')
    masternode_list_rank_command_output = await rpc_connection.masternodelist('rank')
    masternode_list_pubkey_command_output = await rpc_connection.masternodelist('pubkey')
    masternode_list_extra_command_output = await rpc_connection.masternodelist('extra')
    masternode_list_full_df = pd.DataFrame([masternode_list_full_command_output[x].split() for x in masternode_list_full_command_output])
    masternode_list_full_df['txid_vout'] = [x for x in masternode_list_full_command_output]
    masternode_list_full_df.columns = ['supernode_status', 'protocol_version', 'supernode_psl_address', 'lastseentime', 'activeseconds', 'lastpaidtime', 'lastpaidblock', 'ipaddress:port', 'txid_vout']
    masternode_list_full_df.index = masternode_list_full_df['txid_vout']
    masternode_list_full_df.drop(columns=['txid_vout'], inplace=True)
    for current_row in masternode_list_full_df.iterrows():
            current_row_df = pd.DataFrame(current_row[1]).T
            current_txid_vout = current_row_df.index[0]
            current_rank = masternode_list_rank_command_output[current_txid_vout]
            current_pubkey = masternode_list_pubkey_command_output[current_txid_vout]
            current_extra = masternode_list_extra_command_output[current_txid_vout]
            masternode_list_full_df.loc[current_row[0], 'rank'] = current_rank
            masternode_list_full_df.loc[current_row[0], 'pubkey'] = current_pubkey
            masternode_list_full_df.loc[current_row[0], 'extAddress'] = current_extra['extAddress']
            masternode_list_full_df.loc[current_row[0], 'extP2P'] = current_extra['extP2P']
            masternode_list_full_df.loc[current_row[0], 'extKey'] = current_extra['extKey']
            masternode_list_full_df.loc[current_row[0], 'eligibleForMining'] = current_extra['eligibleForMining']
    masternode_list_full_df['lastseentime'] = pd.to_datetime(masternode_list_full_df['lastseentime'], unit='s')
    masternode_list_full_df['lastpaidtime'] = pd.to_datetime(masternode_list_full_df['lastpaidtime'], unit='s')
    masternode_list_full_df['activeseconds'] = masternode_list_full_df['activeseconds'].astype(int)
    masternode_list_full_df['lastpaidblock'] = masternode_list_full_df['lastpaidblock'].astype(int)
    masternode_list_full_df['activedays'] = [float(x)/86400.0 for x in masternode_list_full_df['activeseconds'].values.tolist()]
    masternode_list_full_df['rank'] = masternode_list_full_df['rank'].astype(int)
    masternode_list_full_df__json = masternode_list_full_df.to_json(orient='index')
    return masternode_list_full_df__json


async def get_local_machine_supernode_data_func(rpc_connection):
    local_machine_ip = get_external_ip_func()
    supernode_list_full_df = await check_supernode_list_func(rpc_connection)
    proper_port_number = statistics.mode([x.split(':')[1] for x in supernode_list_full_df['ipaddress:port'].values.tolist()])
    local_machine_ip_with_proper_port = local_machine_ip + ':' + proper_port_number
    local_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['ipaddress:port'] == local_machine_ip_with_proper_port]
    if len(local_machine_supernode_data) == 0:
        logger.error('Local machine is not a supernode!')
        return 0, 0, 0, 0
    else:
        logger.info('Local machine is a supernode!')
        local_sn_rank = local_machine_supernode_data['rank'].values[0]
        local_sn_pastelid = local_machine_supernode_data['extKey'].values[0]
    return local_machine_supernode_data, local_sn_rank, local_sn_pastelid, local_machine_ip_with_proper_port


async def get_sn_data_from_pastelid_func(rpc_connection, specified_pastelid):
    supernode_list_full_df = await check_supernode_list_func(rpc_connection)
    specified_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['extKey'] == specified_pastelid]
    if len(specified_machine_supernode_data) == 0:
        logger.error('Specified machine is not a supernode!')
        return pd.DataFrame()
    else:
        return specified_machine_supernode_data


async def get_sn_data_from_sn_pubkey_func(rpc_connection, specified_sn_pubkey):
    supernode_list_full_df = await check_supernode_list_func(rpc_connection)
    specified_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['pubkey'] == specified_sn_pubkey]
    if len(specified_machine_supernode_data) == 0:
        logger.error('Specified machine is not a supernode!')
        return pd.DataFrame()
    else:
        return specified_machine_supernode_data

def check_if_transparent_psl_address_is_valid_func(pastel_address_string):
    if len(pastel_address_string) == 35 and (pastel_address_string[0:2] == 'Pt'):
        pastel_address_is_valid = 1
    else:
        pastel_address_is_valid = 0
    return pastel_address_is_valid

def check_if_transparent_lsp_address_is_valid_func(pastel_address_string):
    if len(pastel_address_string) == 35 and (pastel_address_string[0:2] == 'tP'):
        pastel_address_is_valid = 1
    else:
        pastel_address_is_valid = 0
    return pastel_address_is_valid

def safe_json_loads_func(json_string: str) -> dict:
    try:
        loaded_json = json.loads(json_string)
        return loaded_json
    except Exception as e:
        logger.error(f"Encountered an error while trying to parse json_string: {e}")
        # Removed the unnecessary replacement of '\/' with '/'
        loaded_json = dirtyjson.loads(json_string.replace('\\"', '"').replace('\\n', ' '))
        return loaded_json

async def get_block_data(rpc_connection, block_height_or_hash):
    global block_data_cache
    if block_height_or_hash in block_data_cache:  # Check if data is in cache
        return block_data_cache[block_height_or_hash]
    block_data = await rpc_connection.getblockheader(block_height_or_hash) # Data not in cache, make RPC call
    block_data_cache[block_height_or_hash] = block_data  # Store data in cache for future use
    return block_data

def find_start_of_extra_data(block_header):
    fixed_header_length = 140
    start_position = fixed_header_length # Start position of the variable-length fields (sPastelID and prevMerkleRootSignature)
    pastel_id_length, bytes_consumed = decode_compact_size(block_header, start_position) # Skip the sPastelID field (standard string serialization with variable-length size prefix)
    start_position += bytes_consumed + pastel_id_length
    signature_length, bytes_consumed = decode_compact_size(block_header, start_position) # Skip the prevMerkleRootSignature field (vector serialization with variable-length size prefix)
    start_position += bytes_consumed + signature_length
    return start_position

def decode_compact_size(data, offset):
    first_byte = data[offset]
    if first_byte < 0xfd:
        return first_byte, 1  # Size is the first byte, 1 byte consumed
    elif first_byte == 0xfd:
        size = int.from_bytes(data[offset + 1:offset + 3], byteorder='little') # Next two bytes are the size
        return size, 3  # Size is next two bytes, 3 bytes consumed
    elif first_byte == 0xfe:
        size = int.from_bytes(data[offset + 1:offset + 5], byteorder='little') # Next four bytes are the size
        return size, 5  # Size is next four bytes, 5 bytes consumed
    else:
        size = int.from_bytes(data[offset + 1:offset + 9], byteorder='little') # Next eight bytes are the size
        return size, 9  # Size is next eight bytes, 9 bytes consumed
    
async def check_block_header_for_supernode_validation_info(rpc_connection, block_height_or_hash):
    try: # Determine if the input is a block height (integer) or a block hash (string)
        if isinstance(block_height_or_hash, int) or (isinstance(block_height_or_hash, str) and block_height_or_hash.isdigit()):
            block_height = int(block_height_or_hash)  # Input is a block height
            block_hash = await rpc_connection.getblockhash(block_height)
        else:
            block_hash = block_height_or_hash  # Input is assumed to be a block hash
        block_header = await get_block_data(rpc_connection, block_hash)  # Use caching function to fetch the block header
        if 'pastelid' in block_header: # Check if the block header contains the PastelID and signature
            supernode_pastelid_pubkey = block_header['pastelid']
            supernode_signature = block_header['prevMerkleRootSignature']
            return supernode_pastelid_pubkey, supernode_signature
        else:
            return "", ""
    except Exception as e:
        logger.error(f"Error in check_block_header_for_supernode_validation_info: {e}")
        return "", ""


async def check_if_supernode_is_eligible_to_sign_block(rpc_connection, supernode_pastelid_pubkey):
    masternode_list_full = await check_supernode_list_func(rpc_connection)
    masternode_list_full_df = pd.DataFrame(safe_json_loads_func(masternode_list_full))
    total_number_of_supernodes = len(masternode_list_full_df)
    getminingeligibility_command_output = await rpc_connection.getminingeligibility()
    getminingeligibility_df = pd.DataFrame(getminingeligibility_command_output['nodes'])
    total_number_of_mining_enabled_supernodes = getminingeligibility_command_output['miningEnabledCount']
    eligible_supernodes_df = getminingeligibility_df[getminingeligibility_df['eligible'] == True]
    total_number_of_currently_eligible_supernodes = len(eligible_supernodes_df)
    lookback_period_in_blocks = math.ceil(total_number_of_mining_enabled_supernodes*.75)
    signing_data_list = [] # Initialize a list to store results
    current_block_height = getminingeligibility_command_output['height']
    # Iterate over the past 'current_number_of_enabled_supernodes' blocks
    for height in range(current_block_height - lookback_period_in_blocks, current_block_height):
        pubkey, signature = await check_block_header_for_supernode_validation_info(rpc_connection, height)
        signing_data_list.append({'block_height': height, 'supernode_pastelid_pubkey': pubkey, 'supernode_signature': signature})
    # Convert the list to DataFrame
    signing_data = pd.DataFrame(signing_data_list)
    # Check if the provided pubkey has signed any of the past blocks
    is_eligible = supernode_pastelid_pubkey in eligible_supernodes_df['mnid'].values
    # Convert DataFrame to a list of dictionaries
    signing_data_list = signing_data.to_dict(orient='records')
    # Find the last block signed by the supernode
    last_signed = signing_data[signing_data['supernode_pastelid_pubkey'] == supernode_pastelid_pubkey].tail(1)
    last_signed_block_height = last_signed['block_height'].iloc[0] if not last_signed.empty else 0
    last_signed_block_hash = await rpc_connection.getblockhash(last_signed_block_height) if last_signed_block_height > 0 else ''
    # Calculate blocks_since_last_signed
    blocks_since_last_signed = current_block_height - last_signed_block_height if last_signed_block_height > 0 else 0
    # Calculate supernode_is_eligible_again_in_n_blocks
    blocks_until_eligibility_restored = math.ceil(total_number_of_mining_enabled_supernodes*0.75) - blocks_since_last_signed if not is_eligible else 0
    # Prepare the response
    return {
        "is_eligible": is_eligible,
        "current_block_height": current_block_height,
        "current_number_of_registered_supernodes": total_number_of_supernodes,
        "current_number_of_mining_enabled_supernodes": total_number_of_mining_enabled_supernodes,
        "current_number_of_eligible_supernodes": total_number_of_currently_eligible_supernodes,
        "last_signed_block_height": last_signed_block_height,
        "last_signed_block_hash": last_signed_block_hash,
        "blocks_since_last_signed": blocks_since_last_signed,
        "blocks_until_eligibility_restored": blocks_until_eligibility_restored
    } 
    
#Misc helper functions:
class MyTimer():
    def __init__(self):
        self.start = time.time()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = time.time()
        runtime = end - self.start
        msg = '({time} seconds to complete)'
        logger.info(msg.format(time=round(runtime, 2)))


def compute_elapsed_time_in_minutes_between_two_datetimes_func(start_datetime, end_datetime):
    time_delta = (end_datetime - start_datetime)
    total_seconds_elapsed = time_delta.total_seconds()
    total_minutes_elapsed = total_seconds_elapsed / 60
    return total_minutes_elapsed


def compute_elapsed_time_in_minutes_since_start_datetime_func(start_datetime):
    end_datetime = datetime.utcnow()
    total_minutes_elapsed = compute_elapsed_time_in_minutes_between_two_datetimes_func(start_datetime, end_datetime)
    return total_minutes_elapsed


def get_sha256_hash_of_input_data_func(input_data_or_string):
    if isinstance(input_data_or_string, str):
        input_data_or_string = input_data_or_string.encode('utf-8')
    sha256_hash_of_input_data = hashlib.sha3_256(input_data_or_string).hexdigest()
    return sha256_hash_of_input_data


def check_if_ip_address_is_valid_func(ip_address_string):
    try:
        _ = ipaddress.ip_address(ip_address_string)
        ip_address_is_valid = 1
    except Exception as e:
        logger.error('Validation Error: ' + str(e))
        ip_address_is_valid = 0
    return ip_address_is_valid


def get_external_ip_func():
    output = os.popen('curl ifconfig.me')
    ip_address = output.read()
    return ip_address


async def check_if_pasteld_is_running_correctly_and_relaunch_if_required_func(rpc_connection):
    pasteld_running_correctly = 0
    try:
        current_pastel_block_number = await get_current_pastel_block_height_func(rpc_connection)
    except Exception as e:
        logger.error(f"Problem running pastel-cli command: {e}")
        current_pastel_block_number = ''
    if isinstance(current_pastel_block_number, int):
        if current_pastel_block_number > 100000:
            pasteld_running_correctly = 1
            logger.info('Pasteld is running correctly!')
    if pasteld_running_correctly == 0:
        logger.info('Pasteld was not running correctly, trying to restart it...')
        process_output = os.system("cd /home/pastelup/ && tmux new -d ./pastelup start walletnode --development-mode")
        logger.info('Pasteld restart command output: ' + str(process_output))
    return pasteld_running_correctly


def install_pasteld_func(network_name='testnet'):
    install_pastelup_script_command_string = "mkdir ~/pastelup && cd ~/pastelup && wget https://github.com/pastelnetwork/pastelup/releases/download/v1.1.3/pastelup-linux-amd64 && mv pastelup-linux-amd64 pastelup && chmod 755 pastelup"
    command_string = f"cd ~/pastelup && ./pastelup install walletnode -n={network_name} --force -r=latest -p=18.118.218.206,18.116.26.219 && \
                        sed -i -e '/hostname/s/localhost/0.0.0.0/' ~/.pastel/walletnode.yml && \
                        sed -i -e '$arpcbind=0.0.0.0' ~/.pastel/pastel.conf && \
                        sed -i -e '$arpcallowip=172.0.0.0/8' ~/.pastel/pastel.conf && \
                        sed -i -e 's/rpcuser=.*/rpcuser=rpc_user/' ~/.pastel/pastel.conf && \
                        sed -i -e 's/rpcpassword=.*/rpcpassword=rpc_pwd/' ~/.pastel/pastel.conf"
    if os.path.exists('~/pastelup/pastelup'):
        logger.info('Pastelup is already installed!')
        logger.info('Running pastelup install command...')
        try:
            command_result = os.system(command_string)
            if not command_result:
                logger.info('Pastelup install command appears to have run successfully!')
        except Exception as e:
            logger.error(f"Error running pastelup install command! Message: {e}; Command result: {command_result}")
    else:
        logger.info('Pastelup is not installed, trying to install it...')
        try:
            install_result = os.system(install_pastelup_script_command_string)
            if not install_result:
                logger.info('Pastelup installed successfully!')
                logger.info('Running pastelup install command...')
                command_result = os.system(command_string)
            else:
                logger.info(f"Pastelup installation failed! Message: {install_result}") 
        except Exception as e:
            logger.error(f"Error running pastelup install command! Message: {e}; Command result: {install_result}")
    return

def initialize_dotenv():
    # Load the environment variables from the .env file
    load_dotenv()


async def initialize_rpc() -> Optional[AsyncAuthServiceProxy]:
    PASTELD_PATH = os.getenv('PASTELD_PATH', '~/.pastel')

    try:
        rpc_host, rpc_port, rpc_user, rpc_password, genpassphrase, other_flags = \
            get_local_rpc_settings_func(os.path.expanduser(PASTELD_PATH))
        rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")
        return rpc_connection
    except Exception as e:
        logger.error(f"Failed to initialize RPC connection: {e}")
        return None
