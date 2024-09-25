from database_code import initialize_db, get_db, SignedPayload, SignedPayloadResponse, BlockHeaderValidationInfo, SupernodeEligibilityResponse, ValidateSignatureResponse, SignaturePack, SignaturePackResponse
from logger_config import setup_logger
import asyncio
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import uvloop
import os
import sys
from uvicorn import Config, Server
from decouple import Config as DecoupleConfig, RepositoryEnv
from services.mining_block_supernode_validator_service import (
    initialize_dotenv,
    initialize_rpc,
    AsyncAuthServiceProxy,
    sign_message_with_pastelid_func,
    sign_base64_encoded_message_with_pastelid_func,
    verify_message_with_pastelid_func,
    verify_base64_encoded_message_with_pastelid_func,
    check_supernode_list_func,
    check_block_header_for_supernode_validation_info,
    check_if_supernode_is_eligible_to_sign_block,
    get_best_block_hash_and_merkle_root_func,
    check_if_blockchain_is_fully_synced,
    periodic_update_task,
    update_sync_status_cache,
)
import yaml
import json
import base64
import random
import aiofiles
from typing import List, Tuple
import itertools
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
import httpx


description_string = "🎢 Pastel's Mining Block Supernode Validator API provides various API endpoints to sign and validate proposed mined blocks and mining shares on the Pastel Blockchain. 💸"
config = DecoupleConfig(RepositoryEnv('.env'))
UVICORN_PORT = config.get("UVICORN_PORT", cast=int)
EXPECTED_AUTH_TOKEN = config.get("AUTH_TOKEN", cast=str)
SLEEP_SECONDS_BETWEEN_PASTELID_ELIGIBILITY_CHECKS = config.get("SLEEP_SECONDS_BETWEEN_PASTELID_ELIGIBILITY_CHECKS", cast=int)
api_key_header_auth = APIKeyHeader(name="Authorization", auto_error=True)
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
pastelid_secrets_dict = {}
supernode_iterator = itertools.cycle([])  # Global round-robin iterator for supernodes
supernode_eligibility_cache = {} # Global cache to store supernode eligibility
logger = setup_logger()
rpc_connection: AsyncAuthServiceProxy = None

DEFAULT_PASTELIDS_FILENAME = 'pastelids_for_sns.yml'

async def load_yaml(file_path):
    async with aiofiles.open(file_path, 'r') as file:
        data = await file.read()
    return yaml.safe_load(data)['all']


async def validate_yaml_data(data):
    if not isinstance(data, dict):
        raise ValueError("YAML data is not a dictionary")
    
    for key, value in data.items():
        if 'pastelid' not in value:
            raise ValueError(f"Missing pastelid in YAML data for key '{key}'")
        if 'pwd' not in value:
            raise ValueError(f"Missing pwd in YAML data for key '{key}'")
        if 'ip_address' not in value:
            raise ValueError(f"Missing ip_address in YAML data for key '{key}'")
        
    logger.info("YAML data validated successfully")
    return True


async def update_supernode_eligibility():
    global rpc_connection
    global supernode_eligibility_cache
    while True:
        start_time = datetime.utcnow()
        current_supernode_json = await check_supernode_list_func(rpc_connection)
        supernode_lookup = build_supernode_lookup(current_supernode_json)
        for pastelid in supernode_lookup.keys():
            eligibility_info = await check_if_supernode_is_eligible_to_sign_block(rpc_connection, pastelid)
            supernode_eligibility_cache[pastelid] = eligibility_info
        end_time = datetime.utcnow()
        elapsed_seconds = (end_time - start_time).total_seconds() + SLEEP_SECONDS_BETWEEN_PASTELID_ELIGIBILITY_CHECKS
        logger.info(f"Supernode eligibility cache updated. Elapsed time since last update: {elapsed_seconds:.2f} seconds. Current time: UTC {end_time}")
        await asyncio.sleep(SLEEP_SECONDS_BETWEEN_PASTELID_ELIGIBILITY_CHECKS)


async def startup_event():
    asyncio.create_task(update_supernode_eligibility())  # Start the background task
    global supernode_iterator
    global pastelid_secrets_dict
    try:
        db_init_complete = await initialize_db()
        logger.info(f"Database initialization complete: {db_init_complete}")

        filepath_to_pastelid_secrets = os.getenv('PASTELIDS_FILENAME', DEFAULT_PASTELIDS_FILENAME)
        pastelid_secrets_dict = await load_yaml(filepath_to_pastelid_secrets)
        await validate_yaml_data(pastelid_secrets_dict)
        supernode_iterator = itertools.cycle(sorted(pastelid_secrets_dict.items(), key=lambda x: x[1]["pastelid"]))  # Initialize the round-robin iterator
    except (FileNotFoundError, yaml.YAMLError) as exc_dict_load:
        logger.error(f"Error loading YAML file: {exc_dict_load}")
        sys.exit(1)
    except ValueError as exc_validate:
        logger.error(f"Error validating YAML data: {exc_validate}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        sys.exit(1)


async def shutdown_event():
    logger.info("Received shutdown signal, cancelling background tasks...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Background tasks cancelled.")


@asynccontextmanager
async def fastapi_lifespan(app: FastAPI):
    try:
        await startup_event()
        yield
    finally:
        await shutdown_event()


app = FastAPI(title="Pastel Mining Block Supernode Validator API", description=description_string, docs_url="/", lifespan=fastapi_lifespan)

allow_all = ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_all,
    allow_credentials=True,
    allow_methods=allow_all,
    allow_headers=allow_all,
    expose_headers=["Authorization"]
)

def build_supernode_lookup(supernode_list_json):
    supernode_dict = json.loads(supernode_list_json)
    lookup = {node.get("extKey"): node for node in supernode_dict.values()}
    return lookup


async def get_signature_from_remote_machine(remote_ip, auth_token):
    url = f"http://{remote_ip}:{UVICORN_PORT}/get_signature_round_robin"
    headers = {"Authorization": auth_token}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200: # Handle non-200 responses appropriately
            return {"error": f"Received status code {response.status_code}"}
        return response.json()


async def get_signature_pack_from_remote_machine(remote_ip, auth_token):
    url = f"http://{remote_ip}:{UVICORN_PORT}/get_signature_pack"
    headers = {"Authorization": auth_token}
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
        if response.status_code != 200: # Handle non-200 responses appropriately
            return {"error": f"Received status code {response.status_code}"}
        return response.json()

  
async def verify_token(api_key: str = Depends(api_key_header_auth)):
    if api_key != EXPECTED_AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return api_key


@app.get("/list_pastelids", response_model=List[str], responses={200: {"description": "List of all registered PastelIDs"}})
async def list_pastelids(token: str = Depends(verify_token)) -> List[str]:
    """
    Retrieve a list of all PastelIDs from the system.
    
    - **token**: Authorization token required.
    
    Returns a list of PastelIDs.
    """
    pastelids = [credentials["pastelid"] for credentials in pastelid_secrets_dict.values()]
    logger.info("List of PastelIDs requested")
    return pastelids


@app.get("/check_block_header_for_supernode_validation_info/{block_height_or_hash}", response_model=BlockHeaderValidationInfo)
async def check_block_header_for_supernode_validation_info_endpoint(block_height_or_hash: str, token: str = Depends(verify_token)):
    """
    Check the block header for supernode validation information based on the provided block height or hash.
    
    - **block_height_or_hash**: Either the block height (integer) or the block hash (string).
    - **token**: Authorization token required.
    
    Returns the supernode PastelID public key and signature if present.
    """    
    try:
        supernode_pastelid_pubkey, supernode_signature = await check_block_header_for_supernode_validation_info(block_height_or_hash)
        return {
            "supernode_pastelid_pubkey": supernode_pastelid_pubkey,
            "supernode_signature": supernode_signature
        }
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Error retrieving block header info: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/check_if_supernode_is_eligible_to_sign_block/{supernode_pastelid_pubkey}", response_model=SupernodeEligibilityResponse)
async def check_if_supernode_is_eligible_to_sign_block_endpoint(supernode_pastelid_pubkey: str, token: str = Depends(verify_token)):
    """
    Check if a supernode is eligible to sign a block using cached data.
    
    - **supernode_pastelid_pubkey**: The PastelID public key of the supernode.
    - **token**: Authorization token required.
    
    Returns detailed eligibility information.
    """
    try:
        # Check if the pastelid is in the eligibility cache
        if supernode_pastelid_pubkey in supernode_eligibility_cache:
            return supernode_eligibility_cache[supernode_pastelid_pubkey]
        else:
            # Optionally compute eligibility if not in cache
            response = await check_if_supernode_is_eligible_to_sign_block(supernode_pastelid_pubkey)
            supernode_eligibility_cache[supernode_pastelid_pubkey] = response
            return response
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Error in checking supernode eligibility: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
@app.get("/get_signature_round_robin", response_model=SignedPayloadResponse)
async def get_signature_round_robin_endpoint(request: Request, db: AsyncSession = Depends(get_db), token: str = Depends(api_key_header_auth)):
    """
    Get a PastelID signature on the best Pastel block's Merkle Root using the next eligible PastelID from the user's list of owned Supernodes in sequence and verify the signature.
    - **token**: Authorization token required.
    Returns a `SignedPayloadResponse` object containing the details of the signed payload.
    """
    global rpc_connection
    for selected_supernode_name, credentials in supernode_iterator:
        pastelid = credentials['pastelid']
        if supernode_eligibility_cache.get(pastelid, False):
            passphrase = credentials['pwd']
            try:
                best_block_hash, best_block_merkle_root, best_block_height = await get_best_block_hash_and_merkle_root_func(rpc_connection)
                signature = await sign_message_with_pastelid_func(rpc_connection, pastelid, best_block_merkle_root, passphrase)
                verification_result = await verify_message_with_pastelid_func(rpc_connection, pastelid, best_block_merkle_root, signature)
                if verification_result:
                    new_signed_payload = SignedPayload(
                        payload_string=best_block_merkle_root,
                        payload_bytes=best_block_merkle_root.encode(),
                        block_signature_payload={
                            'pastelid': pastelid,
                            'signature': signature,
                            'utc_timestamp': str(datetime.utcnow())
                        },
                        requesting_machine_ip_address=request.client.host
                    )
                    db.add(new_signed_payload)
                    await db.commit()
                    return SignedPayloadResponse.model_validate(new_signed_payload)
            except Exception as e:
                logger.error(f'Error signing payload with PastelID {pastelid}: {e}')
    raise HTTPException(status_code=500, detail='Unable to sign payload with any supernode')


# Initialize a cache for the latest signature pack and its block height
latest_cached_signature_pack = None
latest_cached_block_height = None
signature_pack_lock = asyncio.Lock()  # Lock to ensure serialized access to signature generation

@app.get("/get_signature_pack", response_model=SignaturePackResponse)
async def get_signature_pack_endpoint(request: Request, db: AsyncSession = Depends(get_db), token: str = Depends(api_key_header_auth)):
    global rpc_connection, latest_cached_signature_pack, latest_cached_block_height, signature_pack_lock
    best_block_hash, best_block_merkle_root, best_block_height = await get_best_block_hash_and_merkle_root_func(rpc_connection)

    # Check if the block height is the same as the cached one
    if latest_cached_block_height == best_block_height:
        logger.info(f"Returning cached signature pack for block height {best_block_height}")
        return latest_cached_signature_pack

    # Ensure only one thread can generate the signature pack at a time
    async with signature_pack_lock:
        # Double-check the cache after acquiring the lock (in case another thread cached it)
        if latest_cached_block_height == best_block_height:
            logger.info(f"Returning cached signature pack for block height {best_block_height} after acquiring lock")
            return latest_cached_signature_pack
        
        # If block height has changed or no cache, proceed to generate the signature pack
        logger.info(f"Generating signature pack for block height {best_block_height}")
        best_block_merkle_root_byte_vector = bytes.fromhex(best_block_merkle_root)[::-1]
        best_block_merkle_root_byte_vector_base64_encoded = base64.b64encode(best_block_merkle_root_byte_vector).decode('utf-8')
        total_supernodes = len(pastelid_secrets_dict)
    
        async def process_supernode(credentials):
            try:
                pastelid = credentials['pastelid']
                passphrase = credentials['pwd']
                await asyncio.sleep(0.1*random.random())  # Sleep for a short time to avoid RPC issues
                signature = await sign_base64_encoded_message_with_pastelid_func(rpc_connection, pastelid, best_block_merkle_root_byte_vector_base64_encoded, passphrase)
                verification_result = await verify_base64_encoded_message_with_pastelid_func(rpc_connection, pastelid, best_block_merkle_root_byte_vector_base64_encoded, signature)
                if verification_result != "OK":
                    raise ValueError("Verification failed")
                logger.info(f"Signature for PastelID {pastelid} added to signature pack for block height {best_block_height}")
                return pastelid, {
                    'signature': signature,
                    'utc_timestamp': str(datetime.utcnow())
                }
            except Exception as e:
                logger.error(f'Error signing payload with PastelID {pastelid}: {e}')
                return pastelid, None
            
        logger.info(f"Signature pack requested for block height {best_block_height} from {request.client.host}")
        signatures = await asyncio.gather(*(process_supernode(credentials) for _, credentials in itertools.islice(supernode_iterator, total_supernodes)))
        signature_pack_dict = {
            'best_block_height': best_block_height,
            'best_block_hash': best_block_hash,
            'best_block_merkle_root': best_block_merkle_root,
            'requesting_machine_ip_address': request.client.host,
            'signatures': {pastelid: signature for pastelid, signature in signatures if signature is not None}
        }
        logger.info(f"Signature pack for block height {best_block_height} completed")
        signature_pack = SignaturePack(**signature_pack_dict)
        db.add(signature_pack)
        await db.commit()
        await db.refresh(signature_pack)
        
        # Cache the signature pack and update the block height
        latest_cached_signature_pack = SignaturePackResponse.model_validate(signature_pack)
        latest_cached_block_height = best_block_height

    return latest_cached_signature_pack


@app.get("/get_best_block_merkle_root", response_model=str)
async def get_best_block_merkle_root_endpoint(token: str = Depends(verify_token)):
    """
    Get the best Pastel block's Merkle Root.
    
    - **token**: Authorization token required.
    
    Returns the Merkle Root as a string.
    """
    global rpc_connection
    try:
        best_block_hash, best_block_merkle_root, best_block_height = await get_best_block_hash_and_merkle_root_func(rpc_connection)
        return best_block_merkle_root
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Error getting best block Merkle Root: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/get_merkle_signature_from_remote_machine/{remote_ip}", response_model=dict)
async def get_merkle_signature_from_remote_machine_endpoint(remote_ip: str, token: str = Depends(verify_token)):
    """
    Get a Supernode PastelID signature on the best Pastel Block's Merkle Root from a remote machine using round-robin selection of eligible PastelIDs.

    - **remote_ip**: IP address of the remote machine.
    - **token**: Authorization token required.
    
    Returns the JSON response from the remote machine, containing signature details.
    """
    try:
        response = await get_signature_from_remote_machine(remote_ip, token)
        return response
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Error getting signature from remote machine: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/get_block_signature_pack_from_remote_machine/{remote_ip}", response_model=SignaturePackResponse)
async def get_block_signature_pack_from_remote_machine_endpoint(remote_ip: str, token: str = Depends(verify_token)):
    """
    Get a pack of Supernode PastelID signatures on the best Pastel Block's Merkle Root from a remote machine using all configured PastelIDs on that machine.

    - **remote_ip**: IP address of the remote machine.
    - **token**: Authorization token required.
    
    Returns the JSON response from the remote machine, containing all signatures and related metadata, structured as per SignaturePackResponse model.
    """
    try:
        url = f"http://{remote_ip}:{UVICORN_PORT}/get_signature_pack"
        headers = {"Authorization": token}
        timeout = httpx.Timeout(45.0, connect=60.0)  # Set a 45-second timeout for the request
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url, headers=headers)
            response.raise_for_status()  # This will raise an exception for 4XX/5XX responses
            return response.json()  # Assuming the remote response matches the SignaturePackResponse model
    except httpx.HTTPStatusError as http_ex:
        detail = f"HTTP error from remote machine {remote_ip}: Status {http_ex.response.status_code}"
        logger.error(detail)
        return JSONResponse(status_code=http_ex.response.status_code, content={"detail": detail})
    except Exception as e:
        detail = f"Error getting signature pack from remote machine: {e}"
        logger.error(detail)
        raise HTTPException(status_code=500, detail=detail)


@app.get("/validate_supernode_signature", response_model=ValidateSignatureResponse)
async def validate_supernode_signature(
    supernode_pastelid_pubkey: str, 
    supernode_pastelid_signature: str, 
    signed_data_payload: str, 
    token: str = Depends(verify_token)
):
    """
    Validate a signature made by a supernode.

    - **supernode_pastelid_pubkey**: The PastelID public key of the supernode.
    - **supernode_pastelid_signature**: The signature made by the supernode.
    - **signed_data_payload**: The data payload that was signed.
    - **token**: Authorization token required.
    
    Returns a `ValidateSignatureResponse` object with validation result and submitted data.
    """
    global rpc_connection
    try:
        verification_result = await verify_message_with_pastelid_func(
            rpc_connection,
            supernode_pastelid_pubkey,
            signed_data_payload,
            supernode_pastelid_signature
        )
        # Interpret the result as a boolean
        is_signature_valid = verification_result == 'OK'        
        return ValidateSignatureResponse(
            supernode_pastelid_pubkey=supernode_pastelid_pubkey,
            supernode_pastelid_signature=supernode_pastelid_signature,
            signed_data_payload=signed_data_payload,
            is_signature_valid=is_signature_valid
        )
    except Exception as e:
        logger.error(f"Error in validating supernode signature: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/check_if_blockchain_is_fully_synced", response_model=Tuple[bool, str], responses={200: {"description": "Blockchain sync status"}})
async def check_if_blockchain_is_fully_synced_endpoint(token: str = Depends(verify_token), use_optional_checks: int = 0):
    """
    Check if the blockchain is fully synchronized with the network.
    
    - **token**: Authorization token required.
    - **use_optional_checks**: Flag to indicate whether to run additional network quality and peer checks (0 = No, 1 = Yes).
    
    Returns a tuple with a boolean indicating if the blockchain is fully synced and a string providing reasons if it's not fully synced.
    """
    try:
        fully_synced, reasons = await check_if_blockchain_is_fully_synced(use_optional_checks)
        return fully_synced, reasons
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Error checking blockchain sync status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


async def main():
    global rpc_connection

    try:    
        # initialize environment variables
        initialize_dotenv()

        # Initialize the RPC connection
        rpc_connection = await initialize_rpc()

        update_interval_seconds = 60  # Update every 60 seconds
        uvicorn_config = Config(
            app=app,
            host="0.0.0.0",
            port=UVICORN_PORT,
            loop="uvloop",
        )

        # Create the Uvicorn server
        server = Server(uvicorn_config)

        # Create the periodic update task
        update_task = asyncio.create_task(periodic_update_task(rpc_connection, update_interval_seconds))

        # Start the background task to update sync status
        sync_check_task = asyncio.create_task(update_sync_status_cache(rpc_connection))
    except Exception as e:
        logger.error(f"Error during initialization: {e}")
        return

    # Run the server
    await server.serve()

    # Cancel the background tasks when the server is stopped
    update_task.cancel()
    sync_check_task.cancel()
    await asyncio.gather(update_task, sync_check_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
