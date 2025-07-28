import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client
import logging

# Load environment variables from .env file
load_dotenv()

def create_supabase_client(url: str, key: str) -> Client:
    """Create Supabase client with proper handling of version differences"""
    # Patch the httpx Client class to filter out proxy parameter
    try:
        import httpx
        original_init = httpx.Client.__init__
        
        def init_wrapper(self, *args, **kwargs):
            if 'proxy' in kwargs:
                del kwargs['proxy']
            return original_init(self, *args, **kwargs)
        
        # Apply the patch
        httpx.Client.__init__ = init_wrapper
        
        # Now create the client normally
        return create_client(url, key)
    except Exception as e:
        logging.error(f"Error creating Supabase client: {str(e)}")
        # Fallback to basic client creation
        return create_client(url, key)

@dataclass
class OpenAIConfig:
    api_key: str
    base_url: str
    model_name: str
    temperature: float

@dataclass
class SupabaseConfig:
    url: str
    anon_key: str
    service_role_key: str

@dataclass
class DatabaseConfig:
    host: str
    port: str
    database: str
    user: str
    password: str

@dataclass
class EmbeddingConfig:
    api_key: str
    base_url: str
    model: str

@dataclass
class Settings:
    openai: OpenAIConfig
    supabase: SupabaseConfig
    database: DatabaseConfig
    embeddings: EmbeddingConfig        

def get_settings() -> Settings:
    """Get application settings from environment variables"""
    return Settings(
        openai=OpenAIConfig(
            api_key=os.getenv('OPENAI_API_KEY', ''),
            base_url=os.getenv('API_BASE_URL', ''),
            model_name=os.getenv('OPENAI_MODEL_NAME', ''),
            temperature=0
        ),
        supabase=SupabaseConfig(
            url=os.getenv('SUPABASE_URL', ''),
            anon_key=os.getenv('SUPABASE_ANON_KEY', ''),
            service_role_key=os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
        ),
        database=DatabaseConfig(
            host=os.getenv('host', ''),
            port=os.getenv('port', ''),
            database=os.getenv('dbname', ''),
            user=os.getenv('user', ''),
            password=os.getenv('password', '')
        ),
        embeddings=EmbeddingConfig(
            api_key=os.getenv('OPENAI_API_KEY', ''),
            base_url=os.getenv('API_BASE_URL', ''),
            model=os.getenv('EMBEDDING_MODEL_NAME', '')
        )
    )

# Create a global settings instance
settings = get_settings() 