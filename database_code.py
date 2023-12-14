from logger_config import setup_logger
from datetime import datetime
from decimal import Decimal
import traceback
from sqlalchemy import Column, String, DateTime, JSON, LargeBinary, Integer, text as sql_text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from decouple import Config as DecoupleConfig

config = DecoupleConfig(".env")
DATABASE_URL_FOR_SQLITE = config.get("DATABASE_URL_FOR_SQLITE", cast=str, default="sqlite+aiosqlite:///pastel_mining_block_supernode_validator_api.sqlite")
logger = setup_logger()
Base = declarative_base()

class SignedPayload(Base): 
    __tablename__ = 'signed_payload'
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    payload_string = Column(String, index=True)
    payload_bytes = Column(LargeBinary, nullable=False)
    payload_signature = Column(JSON, nullable=False)
    datetime_signed = Column(DateTime, default=datetime.utcnow, index=True)
    requesting_machine_ip_address = Column(String, nullable=False)

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
        tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
        tb_str = "".join(tb_str)        
        logger.error(f"Database Error: {e}\nFull Traceback:\n{tb_str}")
        await db.rollback()
        raise
    finally:
        await db.close()
