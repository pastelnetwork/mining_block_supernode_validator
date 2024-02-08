from database_code import initialize_db, get_db, SignedPayload, SignedPayloadResponse, BlockHeaderValidationInfo, SupernodeEligibilityResponse, ValidateSignatureResponse, SignaturePackResponse
from logger_config import setup_logger
import asyncio
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
import uvloop
from uvicorn import Config, Server
from decouple import Config as DecoupleConfig
from services.mining_block_supernode_validator_service import (sign_message_with_pastelid_func, verify_message_with_pastelid_func, check_supernode_list_func,
                                                            check_block_header_for_supernode_validation_info, check_if_supernode_is_eligible_to_sign_block,
                                                            get_previous_block_hash_and_merkle_root_func, check_if_blockchain_is_fully_synced,
                                                            periodic_update_task, update_sync_status_cache, rpc_connection)
import yaml
import json
import aiofiles
from typing import List, Tuple
import itertools
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
import httpx



description_string = "🎢 Pastel's Mining Block Supernode Validator API provides various API endpoints to sign and validate proposed mined blocks and mining shares on the Pastel Blockchain. 💸"
config = DecoupleConfig(".env")
UVICORN_PORT = config.get("UVICORN_PORT", cast=int)
EXPECTED_AUTH_TOKEN = config.get("AUTH_TOKEN", cast=str)
SLEEP_SECONDS_BETWEEN_PASTELID_ELIGIBILITY_CHECKS = config.get("SLEEP_SECONDS_BETWEEN_PASTELID_ELIGIBILITY_CHECKS", cast=int)
api_key_header_auth = APIKeyHeader(name="Authorization", auto_error=True)
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
pastelid_secrets_dict = {}
supernode_iterator = itertools.cycle([])  # Global round-robin iterator for supernodes
supernode_eligibility_cache = {} # Global cache to store supernode eligibility
logger = setup_logger()
app = FastAPI(title="Pastel Mining Block Supernode Validator API", description=description_string, docs_url="/")

allow_all = ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_all,
    allow_credentials=True,
    allow_methods=allow_all,
    allow_headers=allow_all,
    expose_headers=["Authorization"]
)

filepath_to_pastelid_secrets = 'pastelids_for_testnet_sns.yml'

async def load_yaml(file_path):
    async with aiofiles.open(file_path, 'r') as file:
        data = await file.read()
    return yaml.safe_load(data)['all']

def build_supernode_lookup(supernode_list_json):
    supernode_dict = json.loads(supernode_list_json)
    lookup = {node.get("extKey"): node for node in supernode_dict.values()}
    return lookup

async def update_supernode_eligibility():
    global supernode_eligibility_cache
    while True:
        start_time = datetime.utcnow()
        current_supernode_json = await check_supernode_list_func()
        supernode_lookup = build_supernode_lookup(current_supernode_json)
        for pastelid in supernode_lookup.keys():
            eligibility_info = await check_if_supernode_is_eligible_to_sign_block(pastelid)
            supernode_eligibility_cache[pastelid] = eligibility_info
        end_time = datetime.utcnow()
        elapsed_seconds = (end_time - start_time).total_seconds() + SLEEP_SECONDS_BETWEEN_PASTELID_ELIGIBILITY_CHECKS
        logger.info(f"Supernode eligibility cache updated. Elapsed time since last update: {elapsed_seconds:.2f} seconds. Current time: UTC {end_time}")
        await asyncio.sleep(SLEEP_SECONDS_BETWEEN_PASTELID_ELIGIBILITY_CHECKS)

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
    
        
try:
    pastelid_secrets_dict = load_yaml(filepath_to_pastelid_secrets)
except (FileNotFoundError, yaml.YAMLError) as e:
    logger.error(f"Error loading YAML file: {e}")
    pastelid_secrets_dict = {}

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
    Get a PastelID signature on the previous Pastel block's Merkle Root using the next eligible PastelID from the user's list of owned Supernodes in sequence and verify the signature.
    - **token**: Authorization token required.
    Returns a `SignedPayloadResponse` object containing the details of the signed payload.
    """
    for selected_supernode_name, credentials in supernode_iterator:
        pastelid = credentials['pastelid']
        if supernode_eligibility_cache.get(pastelid, False):
            passphrase = credentials['pwd']
            try:
                previous_block_hash, previous_block_merkle_root, previous_block_height = await get_previous_block_hash_and_merkle_root_func()
                signature = await sign_message_with_pastelid_func(pastelid, previous_block_merkle_root, passphrase)
                verification_result = await verify_message_with_pastelid_func(pastelid, previous_block_merkle_root, signature)
                if verification_result:
                    new_signed_payload = SignedPayload(
                        payload_string=previous_block_merkle_root,
                        payload_bytes=previous_block_merkle_root.encode(),
                        block_signature_payload={
                            'pastelid': pastelid,
                            'signature': signature,
                            'utc_timestamp': str(datetime.utcnow())
                        },
                        requesting_machine_ip_address=request.client.host
                    )
                    db.add(new_signed_payload)
                    await db.commit()
                    return SignedPayloadResponse.from_orm(new_signed_payload)
            except Exception as e:
                logger.error(f'Error signing payload with PastelID {pastelid}: {e}')
    raise HTTPException(status_code=500, detail='Unable to sign payload with any supernode')


@app.get("/get_signature_pack", response_model=SignaturePackResponse)
async def get_signature_pack_endpoint(request: Request, db: AsyncSession = Depends(get_db), token: str = Depends(api_key_header_auth)):
    """
    Get the dict of PastelID signatures on the previous Pastel block's Merkle Root using all of the Supernodes included in the user's config file.
    - **token**: Authorization token required.
    Returns a `SignaturePackResponse` object containing all the signatures as well as related metadata.
    """
    for selected_supernode_name, credentials in supernode_iterator:
        pastelid = credentials['pastelid']
        passphrase = credentials['pwd']
        previous_block_hash, previous_block_merkle_root, previous_block_height = await get_previous_block_hash_and_merkle_root_func()
        signature_pack_dict = {'previous_block_height': previous_block_height,
                            'previous_block_hash': previous_block_hash,
                            'previous_block_merkle_root': previous_block_merkle_root,
                            'requesting_machine_ip_address': request.client.host,
                            'signatures': {}
                            }
        
        signature = await sign_message_with_pastelid_func(pastelid, previous_block_merkle_root, passphrase)
        signature_dict_for_pastelid = {
            'signature': signature,
            'utc_timestamp': str(datetime.utcnow())
        }
        signature_pack_dict['signatures'][pastelid] = signature_dict_for_pastelid
        db.add(signature_pack_dict)
        await db.commit()
        return SignaturePackResponse.from_orm(signature_pack_dict)
    raise HTTPException(status_code=500, detail='Unable to get signature from any supernode')


@app.get("/get_previous_block_merkle_root", response_model=str)
async def get_previous_block_merkle_root_endpoint(token: str = Depends(verify_token)):
    """
    Get the previous Pastel block's Merkle Root.
    
    - **token**: Authorization token required.
    
    Returns the Merkle Root as a string.
    """
    try:
        previous_block_hash, previous_block_merkle_root, previous_block_height = await get_previous_block_hash_and_merkle_root_func()
        return previous_block_merkle_root
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Error getting previous block Merkle Root: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/get_merkle_signature_from_remote_machine/{remote_ip}", response_model=dict)
async def get_merkle_signature_from_remote_machine_endpoint(remote_ip: str, token: str = Depends(verify_token)):
    """
    Get a Supernode PastelID signature on the previous Pastel Block's Merkle Root from a remote machine using round-robin selection of eligible PastelIDs.

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


@app.get("/get_block_signature_pack_from_remote_machine/{remote_ip}", response_model=dict)
async def get_block_signature_pack_from_remote_machine_endpoint(remote_ip: str, token: str = Depends(verify_token)):
    """
    Get a pack of Supernode PastelID signatures on the previous Pastel Block's Merkle Root from a remote machine using all configured PastelIDs on that machine.

    - **remote_ip**: IP address of the remote machine.
    - **token**: Authorization token required.
    
    Returns the JSON response from the remote machine, containing all signatures and related metadata.
    """
    try:
        response = await get_signature_pack_from_remote_machine(remote_ip, token)
        return response
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Error getting signature pack from remote machine: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
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
    try:
        verification_result = await verify_message_with_pastelid_func(
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
    
    
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_supernode_eligibility())  # Start the background task
    global supernode_iterator
    global pastelid_secrets_dict
    try:
        db_init_complete = await initialize_db()
        logger.info(f"Database initialization complete: {db_init_complete}")
        pastelid_secrets_dict = await load_yaml(filepath_to_pastelid_secrets)
        supernode_iterator = itertools.cycle(sorted(pastelid_secrets_dict.items(), key=lambda x: x[1]["pastelid"]))  # Initialize the round-robin iterator
    except Exception as e:
        logger.error(f"Error during startup: {e}")

async def main():
    update_interval_seconds = 60  # Update every 60 seconds
    uvicorn_config = Config(
        "main:app",
        host="0.0.0.0",
        port=UVICORN_PORT,
        loop="uvloop",
    )
    # Create the periodic update task
    update_task = asyncio.create_task(periodic_update_task(rpc_connection, update_interval_seconds))
        # Start the background task to update sync status
    sync_check_task = asyncio.create_task(update_sync_status_cache(rpc_connection))
    # Create the Uvicorn server task
    server = Server(uvicorn_config)
    server_task = server.serve()
    # Run both tasks concurrently
    await asyncio.gather(update_task, sync_check_task, server_task)

if __name__ == "__main__":
    asyncio.run(main())

