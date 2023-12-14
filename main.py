from database_code import initialize_db, SignedPayload, get_db
from logger_config import setup_logger
import asyncio
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
import uvloop
from uvicorn import Config, Server
from decouple import Config as DecoupleConfig
from services.mining_block_supernode_validator_service import (sign_message_with_pastelid_func, verify_message_with_pastelid_func, check_supernode_list_func)
import yaml
import json
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
    if api_key != EXPECTED_AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return api_key

@app.get("/list_pastelids")
async def list_pastelids(token: str = Depends(verify_token)) -> List[str]:
    pastelids = [credentials["pastelid"] for credentials in pastelid_secrets_dict.values()]
    logger.info("List of PastelIDs requested")
    return pastelids


@app.post("/sign_payload")
async def sign_payload(payload: str, request: Request, db: AsyncSession = Depends(get_db), token: str = Depends(api_key_header_auth)):
    block_signature_payload = {"payload_value": payload}
    current_supernode_json = await check_supernode_list_func()
    current_supernode_dict = json.loads(current_supernode_json)
    for _ in range(len(pastelid_secrets_dict)):
        selected_supernode_name, credentials = next(supernode_iterator)
        pastelid = credentials["pastelid"]
        supernode_entry = next((node for key, node in current_supernode_dict.items() if node.get("extKey") == pastelid), None)
        if supernode_entry and supernode_entry.get("supernode_status") == "ENABLED":
            passphrase = credentials["pwd"]
            message_to_sign = payload
            try:
                signature = await sign_message_with_pastelid_func(pastelid, message_to_sign, passphrase)
                message_to_verify = payload
                pastelid_signature_on_message = signature
                verification_result = await verify_message_with_pastelid_func(pastelid, message_to_verify, pastelid_signature_on_message)
                block_signature_payload["pastelid"] = pastelid
                block_signature_payload["signature"] = signature
                block_signature_payload["utc_timestamp"] = str(datetime.utcnow())
                logger.info(f"Signed payload for {selected_supernode_name} with PastelID {pastelid} and verified it: {verification_result}")
                new_signed_payload = SignedPayload(
                    payload_string=payload,
                    payload_bytes=payload.encode(),
                    block_signature_payload=block_signature_payload,
                    requesting_machine_ip_address=request.client.host
                )
                db.add(new_signed_payload)
                await db.commit()               
                return block_signature_payload
            except Exception as e:
                logger.error(f"Error signing payload with PastelID {pastelid}: {e}; trying next supernode.")
    raise HTTPException(status_code=500, detail="Unable to sign payload with any supernode")

@app.on_event("startup")
async def startup_event():
    global supernode_iterator
    try:
        db_init_complete = await initialize_db()
        logger.info(f"Database initialization complete: {db_init_complete}")
        supernode_iterator = itertools.cycle(sorted(pastelid_secrets_dict.items(), key=lambda x: x[1]["pastelid"])) # Initialize the round-robin iterator
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

