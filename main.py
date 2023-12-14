from database_code import initialize_db, get_db, SignedPayload, SignedPayloadResponse, BlockHeaderValidationInfo, SupernodeEligibilityResponse, SignPayloadResponse
from logger_config import setup_logger
import asyncio
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
import uvloop
from uvicorn import Config, Server
from decouple import Config as DecoupleConfig
from services.mining_block_supernode_validator_service import (sign_message_with_pastelid_func, verify_message_with_pastelid_func, check_supernode_list_func,
                                                            check_block_header_for_supernode_validation_info, check_if_supernode_is_eligible_to_sign_block)
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
api_key_header_auth = APIKeyHeader(name="Authorization", auto_error=True)
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
pastelid_secrets_dict = {}
supernode_iterator = itertools.cycle([])  # Global round-robin iterator for supernodes
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
    Check if a supernode is eligible to sign a block.
    
    - **supernode_pastelid_pubkey**: The PastelID public key of the supernode.
    - **token**: Authorization token required.
    
    Returns detailed eligibility information.
    """    
    try:
        response = await check_if_supernode_is_eligible_to_sign_block(supernode_pastelid_pubkey)
        return response
    except HTTPException as http_ex:
        raise http_ex
    except Exception as e:
        logger.error(f"Error in checking supernode eligibility: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    
    
@app.post("/sign_payload", response_model=SignPayloadResponse, responses={200: {"description": "Details of the signed payload"}, 500: {"description": "Internal server error"}})
async def sign_payload(payload: str, request: Request, db: AsyncSession = Depends(get_db), token: str = Depends(api_key_header_auth)):
    """
    Sign a given payload using a PastelID and verify the signature.

    - **payload**: The payload string to be signed.
    - **token**: Authorization token required.
    
    Returns a `SignedPayloadResponse` object containing the details of the signed payload.
    """
    block_signature_payload = {"payload_value": payload}
    current_supernode_json = await check_supernode_list_func()
    supernode_lookup = build_supernode_lookup(current_supernode_json)    
    for selected_supernode_name, credentials in supernode_iterator:
        pastelid = credentials["pastelid"]
        supernode_entry = supernode_lookup.get(pastelid)
        if supernode_entry and supernode_entry.get("supernode_status") == "ENABLED":
            passphrase = credentials["pwd"]
            try:
                signature = await sign_message_with_pastelid_func(pastelid, payload, passphrase)
                verification_result = await verify_message_with_pastelid_func(pastelid, payload, signature)
                if verification_result:
                    # Update the block_signature_payload with signature information
                    block_signature_payload.update({
                        "pastelid": pastelid,
                        "signature": signature,
                        "utc_timestamp": str(datetime.utcnow())
                    })
                    new_signed_payload = SignedPayload(
                        payload_string=payload,
                        payload_bytes=payload.encode(),
                        block_signature_payload=block_signature_payload,
                        requesting_machine_ip_address=request.client.host
                    )
                    db.add(new_signed_payload)
                    await db.commit()
                    # Convert ORM model to Pydantic model for response
                    return SignedPayloadResponse.from_orm(new_signed_payload)
            except Exception as e:
                logger.error(f"Error signing payload with PastelID {pastelid}: {e}")
    raise HTTPException(status_code=500, detail="Unable to sign payload with any supernode")


@app.on_event("startup")
async def startup_event():
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

