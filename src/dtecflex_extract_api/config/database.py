from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.orm import sessionmaker, Session
from src.dtecflex_extract_api.config.database_setup import DATABASE_URL
from fastapi import Depends

from src.dtecflex_extract_api.resources.auth.auth_service import AuthService
from src.dtecflex_extract_api.resources.noticias.noticias_service import NoticiaService

engine = create_engine(DATABASE_URL, connect_args={'use_unicode': True})

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
# Base = declarative_base()

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_noticia_service(db: Session = Depends(get_db)) -> NoticiaService:
    return NoticiaService(session=db)

def get_auth_service(db: Session = Depends(get_db)) -> AuthService:
    return AuthService(session=db)