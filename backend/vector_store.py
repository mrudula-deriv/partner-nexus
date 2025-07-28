from langchain_community.vectorstores import FAISS
from utils import get_openai_embedding_client
from schema_manager import schema_dict_to_chunks, read_schema_metadata
from logging_config import LoggingConfig

# Create logger
logger = LoggingConfig('vector_store').setup_logger()

def initialize_vector_store(supabase_client):
    """Initialize and save the vector store with schema metadata."""
    logger.info("Initializing vector store...")
    embedding = get_openai_embedding_client()
    schema_chunks = schema_dict_to_chunks(read_schema_metadata(supabase_client))
    vectorstore = FAISS.from_texts(schema_chunks, embedding=embedding)
    vectorstore.save_local("metadata/schema_vectorstore")
    logger.info("Vector store initialized and saved.")
    return vectorstore

def load_vector_store():
    """Load the vector store from disk."""
    logger.info("Loading vector store...")
    embedding = get_openai_embedding_client()
    vectorstore = FAISS.load_local(
        "metadata/schema_vectorstore", 
        embedding, 
        allow_dangerous_deserialization=True
    )
    logger.info("Vector store loaded successfully.")
    return vectorstore

def retrieve_context(user_query: str, k: int = 7) -> str:
    """Retrieve relevant schema context for a user query."""
    logger.info(f"Retrieving context for query: {user_query}")
    vectorstore = load_vector_store()
    docs = vectorstore.similarity_search(user_query, k=k)
    context = "\n\n".join([doc.page_content for doc in docs])
    logger.info("Context retrieved successfully.")
    return context 