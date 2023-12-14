from logger_config import setup_logger
import asyncio
import aiohttp
import base64
import decimal
import hashlib
import ipaddress
import json
import os
import platform
import statistics
import time
import warnings
from datetime import datetime, timedelta
from typing import Optional
import dirtyjson
import pandas as pd
import psutil
from httpx import AsyncClient, Limits, Timeout

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

USER_AGENT = "AuthServiceProxy/0.1"
HTTP_TIMEOUT = 180
active_sessions = {}  # Dictionary to hold active sessions and their last used times
        
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
        connector = aiohttp.TCPConnector(limit=1000)
        self.client_session = aiohttp.ClientSession(connector=connector)
        return self.client_session

    async def close_session(self):
        if self.client_session:
            await self.client_session.close()
            self.client_session = None

session_manager = ClientSessionManager() # Initialize global session manager
        
def parse_mime_type(mime_type):
    return tuple(mime_type.split(';')[0].split('/'))
        
def get_local_rpc_settings_func(directory_with_pastel_conf=os.path.expanduser("~/.pastel/")):
    with open(os.path.join(directory_with_pastel_conf, "pastel.conf"), 'r') as f:
        lines = f.readlines()
    other_flags = {}
    rpchost = '127.0.0.1'
    rpcport = '19932'
    for line in lines:
        if line.startswith('rpcport'):
            value = line.split('=')[1]
            rpcport = value.strip()
        elif line.startswith('rpcuser'):
            value = line.split('=')[1]
            rpcuser = value.strip()
        elif line.startswith('rpcpassword'):
            value = line.split('=')[1]
            rpcpassword = value.strip()
        elif line.startswith('rpchost'):
            pass
        elif line == '\n':
            pass
        else:
            current_flag = line.strip().split('=')[0].strip()
            current_value = line.strip().split('=')[1].strip()
            other_flags[current_flag] = current_value
    return rpchost, rpcport, rpcuser, rpcpassword, other_flags
    
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
        return float(round(o, 8))
    raise TypeError(repr(o) + " is not JSON serializable")
    
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
        
async def get_current_pastel_block_height_func():
    global rpc_connection
    best_block_hash = await rpc_connection.getbestblockhash()
    best_block_details = await rpc_connection.getblock(best_block_hash)
    curent_block_height = best_block_details['height']
    return curent_block_height

async def get_previous_block_hash_and_merkle_root_func():
    global rpc_connection
    previous_block_height = await get_current_pastel_block_height_func()
    previous_block_hash = await rpc_connection.getblockhash(previous_block_height)
    previous_block_details = await rpc_connection.getblock(previous_block_hash)
    previous_block_merkle_root = previous_block_details['merkleroot']
    return previous_block_hash, previous_block_merkle_root, previous_block_height

async def get_last_block_data_func():
    global rpc_connection
    current_block_height = await get_current_pastel_block_height_func()
    block_data = await rpc_connection.getblock(str(current_block_height))
    return block_data

async def check_psl_address_balance_func(address_to_check):
    global rpc_connection
    balance_at_address = await rpc_connection.z_getbalance(address_to_check) 
    return balance_at_address

async def get_raw_transaction_func(txid):
    global rpc_connection
    raw_transaction_data = await rpc_connection.getrawtransaction(txid, 1) 
    return raw_transaction_data
        
async def sign_message_with_pastelid_func(pastelid, message_to_sign, passphrase) -> str:
    global rpc_connection
    results_dict = await rpc_connection.pastelid('sign', message_to_sign, pastelid, passphrase, 'ed448')
    return results_dict['signature']

async def verify_message_with_pastelid_func(pastelid, message_to_verify, pastelid_signature_on_message) -> str:
    global rpc_connection
    verification_result = await rpc_connection.pastelid('verify', message_to_verify, pastelid_signature_on_message, pastelid, 'ed448')
    return verification_result['verification']

async def check_masternode_top_func():
    global rpc_connection
    masternode_top_command_output = await rpc_connection.masternode('top')
    return masternode_top_command_output

async def check_supernode_list_func():
    global rpc_connection
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
    masternode_list_full_df['lastseentime'] = pd.to_datetime(masternode_list_full_df['lastseentime'], unit='s')
    masternode_list_full_df['lastpaidtime'] = pd.to_datetime(masternode_list_full_df['lastpaidtime'], unit='s')
    masternode_list_full_df['activeseconds'] = masternode_list_full_df['activeseconds'].astype(int)
    masternode_list_full_df['lastpaidblock'] = masternode_list_full_df['lastpaidblock'].astype(int)
    masternode_list_full_df['activedays'] = [float(x)/86400.0 for x in masternode_list_full_df['activeseconds'].values.tolist()]
    masternode_list_full_df['rank'] = masternode_list_full_df['rank'].astype(int)
    masternode_list_full_df__json = masternode_list_full_df.to_json(orient='index')
    return masternode_list_full_df__json
    
async def get_local_machine_supernode_data_func():
    local_machine_ip = get_external_ip_func()
    supernode_list_full_df = await check_supernode_list_func()
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

async def get_sn_data_from_pastelid_func(specified_pastelid):
    supernode_list_full_df = await check_supernode_list_func()
    specified_machine_supernode_data = supernode_list_full_df[supernode_list_full_df['extKey'] == specified_pastelid]
    if len(specified_machine_supernode_data) == 0:
        logger.error('Specified machine is not a supernode!')
        return pd.DataFrame()
    else:
        return specified_machine_supernode_data

async def get_sn_data_from_sn_pubkey_func(specified_sn_pubkey):
    supernode_list_full_df = await check_supernode_list_func()
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
        loaded_json = dirtyjson.loads(json_string.replace('\\"', '"').replace('\/', '/').replace('\\n', ' '))
        return loaded_json

async def get_block_data(block_height_or_hash):
    global block_data_cache
    if block_height_or_hash in block_data_cache:  # Check if data is in cache
        return block_data_cache[block_height_or_hash]
    block_data = await rpc_connection.getblockheader(block_height_or_hash) # Data not in cache, make RPC call
    block_data_cache[block_height_or_hash] = block_data  # Store data in cache for future use
    return block_data

async def check_block_header_for_supernode_validation_info(block_height_or_hash):
    global rpc_connection
    try: # Determine if the input is a block height (integer) or a block hash (string)
        if isinstance(block_height_or_hash, int) or (isinstance(block_height_or_hash, str) and block_height_or_hash.isdigit()):
            block_height = int(block_height_or_hash)  # Input is a block height
            block_hash = await rpc_connection.getblockhash(block_height)
        else:  # Input is assumed to be a block hash
            block_hash = block_height_or_hash
        block_header = await get_block_data(block_hash)  # Use caching function to fetch the block header
        if len(block_header) > 140: # Check if the block header has extra data beyond 140 bytes
            extra_data = block_header[140:].decode('utf-8') # Extract and decode the data
            parts = extra_data.split('|') # Attempt to split the data into supernode_pastelid_pubkey and supernode_signature
            if len(parts) == 2:
                supernode_pastelid_pubkey = parts[0]
                supernode_signature = parts[1]
            else: # Handle the case where there is no delimiter
                pubkey_length = 86 # len("jXYmHy1uLXuyaKq1YmkFPXd47i3jhqn7b4t7ytNvQr9xQyhyPVaKiQqBu4Knhcp3K9CHKPStWHbEEZLJjCTCzG")
                supernode_pastelid_pubkey = extra_data[:pubkey_length]
                supernode_signature = extra_data[pubkey_length:]
            return supernode_pastelid_pubkey, supernode_signature
        else: # No extra data beyond 140 bytes
            return "", ""
    except Exception as e:
        logger.error(f"Error in check_block_header_for_supernode_validation_info: {e}")
        return "", ""

async def check_if_supernode_is_eligible_to_sign_block(supernode_pastelid_pubkey):
    # Get the supernode list
    supernode_list_json = await check_supernode_list_func()
    supernode_list_df = pd.read_json(supernode_list_json, orient='index')
    # Count the number of ENABLED supernodes
    enabled_supernodes = supernode_list_df[supernode_list_df['supernode_status'] == 'ENABLED']
    current_number_of_enabled_supernodes = len(enabled_supernodes)
    # Initialize a list to store results
    signing_data_list = []
    # Fetch current block height
    current_block_height = await get_current_pastel_block_height_func()
    # Iterate over the past 'current_number_of_enabled_supernodes' blocks
    for height in range(current_block_height - current_number_of_enabled_supernodes, current_block_height):
        pubkey, signature = await check_block_header_for_supernode_validation_info(height)
        signing_data_list.append({'block_height': height, 'supernode_pastelid_pubkey': pubkey, 'supernode_signature': signature})
    # Convert the list to DataFrame
    signing_data = pd.DataFrame(signing_data_list)
    # Check if the provided pubkey has signed any of the past blocks
    is_eligible = supernode_pastelid_pubkey in signing_data['supernode_pastelid_pubkey'].values
    # Convert DataFrame to a list of dictionaries
    signing_data_list = signing_data.to_dict(orient='records')
    # Check if the provided pubkey has signed any of the past blocks
    is_eligible = supernode_pastelid_pubkey in signing_data['supernode_pastelid_pubkey'].values
    # Check if the provided pubkey has signed any of the past blocks
    is_eligible = supernode_pastelid_pubkey not in signing_data['supernode_pastelid_pubkey'].values
    # Find the last block signed by the supernode
    last_signed = signing_data[signing_data['supernode_pastelid_pubkey'] == supernode_pastelid_pubkey].tail(1)
    last_signed_block_height = last_signed['block_height'].iloc[0] if not last_signed.empty else 0
    last_signed_block_hash = await rpc_connection.getblockhash(last_signed_block_height) if last_signed_block_height > 0 else ''
    # Calculate blocks_since_last_signed
    blocks_since_last_signed = current_block_height - last_signed_block_height if last_signed_block_height > 0 else 0
    # Calculate supernode_is_eligible_again_in_n_blocks
    blocks_until_eligibility_restored  = current_number_of_enabled_supernodes - blocks_since_last_signed if not is_eligible else 0
    # Prepare the response
    return {
        "is_eligible": is_eligible,
        "signing_data": signing_data_list,
        "current_block_height": current_block_height,
        "current_number_of_enabled_supernodes": current_number_of_enabled_supernodes,
        "last_signed_block_height": last_signed_block_height,
        "last_signed_block_hash": last_signed_block_hash,
        "blocks_since_last_signed": blocks_since_last_signed,
        "blocks_until_eligibility_restored ": blocks_until_eligibility_restored 
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


async def check_if_pasteld_is_running_correctly_and_relaunch_if_required_func():
    pasteld_running_correctly = 0
    try:
        current_pastel_block_number = await get_current_pastel_block_height_func()
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
            

#_______________________________________________________________


rpc_host, rpc_port, rpc_user, rpc_password, other_flags = get_local_rpc_settings_func()
rpc_connection = AsyncAuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")


