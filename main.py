from database_code import initialize_db, get_db, SignedPayload, SignedPayloadResponse, BlockHeaderValidationInfo, SupernodeEligibilityResponse, ValidateSignatureResponse
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
                                                            get_previous_block_hash_and_merkle_root_func)
import yaml
import json
import aiofiles
from typing import List
import itertools
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession

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
async def sign_payload_round_robin_endpoint(request: Request, db: AsyncSession = Depends(get_db), token: str = Depends(api_key_header_auth)):
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
    uvicorn_config = Config(
        "main:app",
        host="0.0.0.0",
        port=UVICORN_PORT,
        loop="uvloop",
    )
    server = Server(uvicorn_config)
    await server.serve()
    

if __name__ == "__main__":
    asyncio.run(main())

