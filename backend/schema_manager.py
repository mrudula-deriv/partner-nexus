import json
import os, time
from datetime import datetime, timedelta
from typing import Dict, List
from supabase import Client
from logging_config import LoggingConfig

# Create logger
logger = LoggingConfig('schema_manager').setup_logger()

def get_multi_schema_metadata(supabase_client: Client, schemas: List[str]) -> str:
    """
    Fetches metadata for the given schemas by calling the Supabase RPC function,
    formats it, and saves it to a JSON file.
    """
    logger.info("Fetching metadata for schemas: %s", schemas)
    response = supabase_client.rpc("get_schema_metadata", {"p_schema_names_json": schemas}).execute()

    rows = response.data
    if not rows:
        logger.warning("Metadata fetch returned no data.")
        rows = []

    logger.info("Successfully retrieved metadata.")

    schema_dict = {}
    for table_data in rows:
        schema = table_data.get('table_schema')
        table = table_data.get('table_name')
        if not schema or not table:
            continue
            
        key = f"{schema}.{table}"
        schema_dict[key] = {
            "columns": table_data.get('columns', []),
            "relationships": [],  # Initialize empty relationships list
            "description": table_data.get('table_description')
        }
    
    # Infer and add relationships directly to the schema metadata
    schema_dict_with_relationships = infer_table_relationships(schema_dict)
    
    # Create a directory for storing metadata if it doesn't exist
    if not os.path.exists('metadata'):
        os.makedirs('metadata')
    
    # Write schema data to JSON file
    json_file_path = 'metadata/schema_metadata.json'
    with open(json_file_path, 'w') as f:
        json.dump(schema_dict_with_relationships, f, indent=4)
    
    return json_file_path

def read_schema_metadata(supabase_client: Client, max_age_hours: int = 24) -> Dict:
    """Read schema metadata with automatic refresh based on file age."""
    json_file_path = 'metadata/schema_metadata.json'
    should_refresh = False
    
    if not os.path.exists(json_file_path):
        should_refresh = True
        logger.info("Schema metadata file not found, fetching fresh data...")
    else:
        # Check file age
        file_mtime = os.path.getmtime(json_file_path)
        file_age = time.time() - file_mtime
        max_age_seconds = max_age_hours * 3600
        
        if file_age > max_age_seconds:
            should_refresh = True
            logger.info(f"Schema metadata is {file_age/3600:.1f} hours old, refreshing...")
    
    if should_refresh:
        logger.info("Refreshing schema metadata from Supabase...")
        get_multi_schema_metadata(supabase_client, ['partner', 'client', 'gp'])
    
    with open(json_file_path, 'r') as f:
        schema_dict = json.load(f)
    return schema_dict
def infer_table_relationships(schema_dict: Dict) -> Dict:
    """
    Infers table relationships from the schema based on naming conventions.
    Adds the inferred relationships directly to the schema_dict and returns it.
    Also saves the relationships to a separate JSON file for backward compatibility.
    """
    logger.info("Inferring table relationships from schema...")
    
    primary_keys = {}
    for table_name, table_data in schema_dict.items():
        for column in table_data['columns']:
            if column.get('is_primary_key'):
                # Store as {column_name: table_name} for easy lookup
                pk_col_name = column.get('column_name')
                if pk_col_name:
                    primary_keys[pk_col_name] = table_name

    # Create a copy of schema_dict to modify
    updated_schema_dict = schema_dict.copy()
    relationships_dict = {}  # For backward compatibility
    
    for table_name, table_data in schema_dict.items():
        relations = []
        for column in table_data['columns']:
            col_name = column.get('column_name')
            if not col_name or column.get('is_primary_key'):
                continue

            # Heuristic 1: column name is a primary key in another table
            if col_name in primary_keys and primary_keys[col_name] != table_name:
                relation = {
                    'foreign_key_column': col_name,
                    'references_table': primary_keys[col_name],
                    'references_column': col_name,
                    'constraint_name': f"inferred_fk_{table_name.replace('.', '_')}_{col_name}"
                }
                relations.append(relation)
                continue
            
            # Heuristic 2: column name of the form `..._id` or `..._pk`
            if col_name.endswith(('_id', '_pk')):
                # Try to find a table that matches the prefix
                prefix = col_name.rsplit('_', 1)[0]
                # Look for tables named `prefix` or `prefix` + 's' (plural)
                for pk_name, pk_table_name in primary_keys.items():
                    pk_table_base_name = pk_table_name.split('.')[-1]
                    if pk_table_base_name == prefix or pk_table_base_name == prefix + 's':
                        relation = {
                            'foreign_key_column': col_name,
                            'references_table': pk_table_name,
                            'references_column': pk_name,
                            'constraint_name': f"inferred_fk_{table_name.replace('.', '_')}_{col_name}"
                        }
                        relations.append(relation)

        # Add relationships to the schema metadata
        if relations:
            updated_schema_dict[table_name]['relationships'] = relations
            relationships_dict[table_name] = relations  # For backward compatibility

    logger.info("Successfully inferred relationships.")
    
    return updated_schema_dict

def schema_dict_to_chunks(schema_dict: Dict) -> List[str]:
    """Convert schema dictionary to text chunks for embedding."""
    chunks = []
    for table, meta in schema_dict.items():
        columns = meta.get("columns", [])
        relationships = meta.get("relationships", [])
        desc = meta.get("description", "")
        column_text = "\n".join(
            f"- {col['column_name']} ({col['data_type']})"
            for col in columns
        )
        rel_text = "\n".join(
            f"- {r['foreign_key_column']} → {r['references_table']}.{r['references_column']}"
            for r in relationships
        )
        chunk = f"""
        Table: {table}
        Description: {desc}
        Columns:
        {column_text}
        Relationships:
        {rel_text}
        """
        chunks.append(chunk.strip())
    return chunks 