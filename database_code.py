from logger_config import setup_logger
from datetime import datetime
from decimal import Decimal
import traceback
from sqlalchemy import Column, String, DateTime, JSON, LargeBinary, Integer, text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from decouple import Config as DecoupleConfig
from pydantic import BaseModel
from typing import Optional, Dict

config = DecoupleConfig(".env")
DATABASE_URL_FOR_SQLITE = config.get("DATABASE_URL_FOR_SQLITE", cast=str, default="sqlite+aiosqlite:///pastel_mining_block_supernode_validator_api.sqlite")
logger = setup_logger()
Base = declarative_base()

class SignedPayload(Base): 
    __tablename__ = 'signed_payload'
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    payload_string = Column(String, index=True)
    payload_bytes = Column(LargeBinary, nullable=False)
    block_signature_payload = Column(JSON, nullable=False)
    datetime_signed = Column(DateTime, default=datetime.utcnow, index=True)
    requesting_machine_ip_address = Column(String, nullable=False)
    
class SignedPayloadResponse(BaseModel):
    id: int
    payload_string: str
    payload_bytes: bytes
    pastelid: str
    signature: str
    utc_timestamp: str
    datetime_signed: str
    requesting_machine_ip_address: str

    @classmethod
    def model_validate(cls, model: SignedPayload):
        """
        Convert SignedPayload ORM model to Pydantic model.
        """
        return cls(
            id=model.id,
            payload_string=model.payload_string,
            payload_bytes=model.payload_bytes,
            pastelid=model.block_signature_payload.get('pastelid', ''),
            signature=model.block_signature_payload.get('signature', ''),
            utc_timestamp=model.block_signature_payload.get('utc_timestamp', ''),
            datetime_signed=model.datetime_signed.isoformat(),
            requesting_machine_ip_address=model.requesting_machine_ip_address
        )

    class Config:
        from_attributes = True  # Allow reading attributes from ORM objects

class BlockHeaderValidationInfo(BaseModel):
    supernode_pastelid_pubkey: str
    supernode_signature: str

class SupernodeEligibilityResponse(BaseModel):
    is_eligible: bool
    current_block_height: int
    current_number_of_registered_supernodes: int
    current_number_of_mining_enabled_supernodes: int
    current_number_of_eligible_supernodes: int
    last_signed_block_height: Optional[int] = None
    last_signed_block_hash: Optional[str] = None
    blocks_since_last_signed: Optional[int] = None
    blocks_until_eligibility_restored : int = 0

class SignPayloadResponse(BaseModel):
    payload_value: str
    pastelid: str
    signature: str
    utc_timestamp: str
    
class SignaturePack(Base):
    __tablename__ = 'signature_packs'
    id = Column(Integer, primary_key=True, index=True)
    best_block_height = Column(Integer)
    best_block_hash = Column(String)
    best_block_merkle_root = Column(String)
    requesting_machine_ip_address = Column(String)
    signatures = Column(JSON)  # Storing JSON directly; ensure your DB supports JSON columns    
    
class SignatureDetails(BaseModel):
    signature: str
    utc_timestamp: datetime

class SignaturePackResponse(BaseModel):
    best_block_height: int
    best_block_hash: str
    best_block_merkle_root: str
    requesting_machine_ip_address: str
    signatures: Dict[str, SignatureDetails]

    class Config:
        from_attributes = True  # This enables the model to work with ORM objects
            
def to_serializable(val):
    if isinstance(val, datetime):
        return val.isoformat()
    elif isinstance(val, Decimal):
        return float(val)
    else:
        return str(val)

def to_dict(self):
    d = {}
    for column in self.__table__.columns:
        if not isinstance(column.type, LargeBinary):
            value = getattr(self, column.name)
            if value is not None:
                serialized_value = to_serializable(value)
                d[column.name] = serialized_value if serialized_value is not None else value
    return d

class ValidateSignatureResponse(BaseModel):
    supernode_pastelid_pubkey: str
    supernode_pastelid_signature: str
    signed_data_payload: str
    is_signature_valid: bool
    
SignedPayload.to_dict = to_dict

engine = create_async_engine(DATABASE_URL_FOR_SQLITE, echo=False, connect_args={"check_same_thread": False})
    
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

async def initialize_db():
    list_of_sqlite_pragma_strings = [
        "PRAGMA journal_mode=WAL;", 
        "PRAGMA synchronous = NORMAL;", 
        "PRAGMA cache_size = -262144;", 
        "PRAGMA busy_timeout = 2000;", 
        "PRAGMA wal_autocheckpoint = 100;"
    ]
    list_of_sqlite_pragma_justification_strings = [
        "Set SQLite to use Write-Ahead Logging (WAL) mode (from default DELETE mode) so that reads and writes can occur simultaneously",
        "Set synchronous mode to NORMAL (from FULL) so that writes are not blocked by reads",
        "Set cache size to 1GB (from default 2MB) so that more data can be cached in memory and not read from disk; to make this 256MB, set it to -262144 instead",
        "Increase the busy timeout to 2 seconds so that the database waits",
        "Set the WAL autocheckpoint to 100 (from default 1000) so that the WAL file is checkpointed more frequently"
    ]
    assert(len(list_of_sqlite_pragma_strings) == len(list_of_sqlite_pragma_justification_strings))
    try:
        async with engine.begin() as conn:
            for pragma_string in list_of_sqlite_pragma_strings:
                await conn.execute(sql_text(pragma_string))
            await conn.run_sync(Base.metadata.create_all)  # Create tables if they don't exist
        await engine.dispose()
        return True
    except Exception as e:
        logger.error(f"Database Initialization Error: {e}")
        return False
    
async def get_db():
    db = AsyncSessionLocal()
    try:
        yield db
        await db.commit()
    except Exception as e:
        tb_str = traceback.format_exception(type(e), e, e.__traceback__)
        tb_str = "".join(tb_str)        
        logger.error(f"Database Error: {e}\nFull Traceback:\n{tb_str}")
        await db.rollback()
        raise
    finally:
        await db.close()
