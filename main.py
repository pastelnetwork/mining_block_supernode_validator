from database_code import initialize_db, SignedNonce, get_db
from logger_config import setup_logger
import asyncio
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
import uvloop
from uvicorn import Config, Server
from decouple import Config as DecoupleConfig
from services.mining_nonce_validator_service import (sign_message_with_pastelid_func, verify_message_with_pastelid_func, check_supernode_list_func)
import yaml
import json
from typing import List
from sqlalchemy.ext.asyncio import AsyncSession

description_string = "🎢 Pastel's Mining Nonce Validator API provides various API endpoints to sign and validate proposed mined blocks and mining shares on the Pastel Blockchain. 💸"
config = DecoupleConfig(".env")
UVICORN_PORT = config.get("UVICORN_PORT", cast=int)
EXPECTED_AUTH_TOKEN = config.get("AUTH_TOKEN", cast=str)
api_key_header_auth = APIKeyHeader(name="Authorization", auto_error=True)
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
logger = setup_logger()
app = FastAPI(title="Pastel Mining Nonce Validator API", description=description_string, docs_url="/")

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

def load_yaml(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data['all']

try:
    pastelid_secrets_dict = load_yaml(filepath_to_pastelid_secrets)
except (FileNotFoundError, yaml.YAMLError) as e:
    logger.error(f"Error loading YAML file: {e}")
    pastelid_secrets_dict = {}

async def verify_token(api_key: str = Depends(api_key_header_auth)):
    if api_key != f"Bearer {EXPECTED_AUTH_TOKEN}":
        raise HTTPException(status_code=401, detail="Unauthorized")
    return api_key

@app.get("/list_pastelids")
async def list_pastelids(token: str = Depends(verify_token)) -> List[str]:
    pastelids = [credentials["pastelid"] for credentials in pastelid_secrets_dict.values()]
    logger.info("List of PastelIDs requested")
    return pastelids

@app.post("/sign_nonce")
async def sign_nonce(nonce: str, request: Request, db: AsyncSession = Depends(get_db), token: str = Depends(api_key_header_auth)):
    nonce_signature_payload = {"nonce_value": nonce, "signatures": {}}
    current_supernode_json = await check_supernode_list_func()
    current_supernode_dict = json.loads(current_supernode_json)
    for supernode_name, credentials in pastelid_secrets_dict.items():
        pastelid = credentials["pastelid"]
        supernode_entry = next((node for key, node in current_supernode_dict.items() if node.get("extKey") == pastelid), None) # Find the supernode entry by matching the PastelID (extKey)
        if supernode_entry and supernode_entry.get("supernode_status") == "ENABLED": # Check if the supernode's pastelid is found and its status is ENABLED
            passphrase = credentials["pwd"]
            message_to_sign = nonce
            try:
                signature = await sign_message_with_pastelid_func(pastelid, message_to_sign, passphrase)
                message_to_verify = nonce
                pastelid_signature_on_message = signature
                verification_result = await verify_message_with_pastelid_func(pastelid, message_to_verify, pastelid_signature_on_message)
                nonce_signature_payload["signatures"][pastelid] = signature
                logger.info(f"Signed nonce for {supernode_name} with PastelID {pastelid} and verified it: {verification_result}")
                new_signed_nonce = SignedNonce(
                    nonce_string=nonce,
                    nonce_bytes=nonce.encode(),
                    nonce_signature_payload=nonce_signature_payload,
                    requesting_machine_ip_address=request.client.host
                )
                db.add(new_signed_nonce)
            except Exception as e:
                logger.error(f"Error signing nonce for {supernode_name} with PastelID {pastelid}: {e}; going to skip signing for this supernode!")
        else:
            logger.info(f"Skipping signing for {supernode_name} as it is not listed or not ENABLED")
    await db.commit() # Commit the transaction after processing all signatures               
    return nonce_signature_payload

async def startup():
    try:
        db_init_complete = await initialize_db()
        logger.info(f"Database initialization complete: {db_init_complete}")
    except Exception as e:
        logger.error(f"Error during database initialization: {e}")

@app.on_event("startup")
async def startup_event():
    await startup()

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

