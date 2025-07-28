from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from supabase import create_client, Client
import psycopg2
from config import settings, create_supabase_client

def get_openai_client(temperature: float = 0) -> ChatOpenAI:
    """Get configured OpenAI client"""
    return ChatOpenAI(
        api_key=settings.openai.api_key,
        base_url=settings.openai.base_url,
        model_name=settings.openai.model_name,
        temperature=temperature
    )

def get_supabase_client() -> Client:
    """Get configured Supabase client"""
    return create_supabase_client(
        settings.supabase.url,
        settings.supabase.service_role_key
    )

def get_db_connection():
    """Get configured database connection"""
    return psycopg2.connect(
        host=settings.database.host,
        port=settings.database.port,
        dbname=settings.database.database,
        user=settings.database.user,
        password=settings.database.password
    ) 

def get_openai_embedding_client() -> OpenAIEmbeddings:
    """Get configured OpenAI embedding client"""
    if not settings.embeddings.api_key:
        raise ValueError("OPENAI_API_KEY environment variable is required but not set")
    
    return OpenAIEmbeddings(
        api_key=settings.embeddings.api_key,
        base_url=settings.embeddings.base_url if settings.embeddings.base_url else None,
        model=settings.embeddings.model
    )