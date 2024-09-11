import re
from glob import glob
from pathlib import Path

from neo4j import GraphDatabase
from sql_metadata import Parser

DATA_LINEAGE = {}


URI = "neo4j://localhost"
AUTH = ("neo4j", "hellohello")

def verify_neo4j_connection():
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()

def run_neo4j_statements(statement_list: list[str]) -> None:
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        for statement in statement_list:
            with driver.session() as session:
                session.run(statement)

def clean_query(query: str):
    query = query.lower()
    query = query.replace('\n', ' ').replace('\r', ' ')
    query = re.sub(' +', ' ', query)
    return query

def clean_table_name(table_name: str):
    table_name = table_name.lower()
    table_name = table_name.replace('\n', '').replace('\r', '').replace(' ', '_')
    return table_name

def extract_table_creation(query: str, file_path: str):
    query = clean_query(query)
    create_table = re.search(r'create table (\w+)', query)
    if create_table:
        return clean_table_name(create_table.group(1))
    return clean_table_name(Path(file_path).stem)

def generate_create_neo4j_statement():
    neo4j_statments = []
    
    for key, value in DATA_LINEAGE.items():
        create_statement = "CREATE ( " + key + ":Table {name: '" + key + "'})"
        neo4j_statments.append(create_statement)

        for table in value:
            create_statement = "CREATE ( " + table + ":Table {name: '" + table + "'})"
            neo4j_statments.append(create_statement)
    return list(set(neo4j_statments))

def generate_relationship_neo4j_statement():
    neo4j_statments = []
    
    for key, value in DATA_LINEAGE.items():
        for table in value:
            relationship_statement = f'MATCH (a:Table), (b:Table) WHERE a.name = "{key}" AND b.name = "{table}" CREATE (a)-[r:DEPENDS_ON]->(b)'
            
            neo4j_statments.append(relationship_statement)
    
    return list(set(neo4j_statments))

def get_all_sql_files(path_to_sql_files: str="examples/**.sql"):
    sql_files = glob(path_to_sql_files)
    return sql_files

def get_model_dependencies(query: str):
    used_tables = Parser(query).tables
    clean_tables = [table.split(".")[-1] for table in used_tables]
    return clean_tables

def main():
    verify_neo4j_connection()
    
    sql_files = get_all_sql_files()
    for sql_file in sql_files:
        with open(sql_file, "r") as f:
            query = f.read()
            created_table = extract_table_creation(query=query, file_path=sql_file)
            dependency_tables = get_model_dependencies(query=query)
            DATA_LINEAGE[created_table] = dependency_tables
    
    neo4j_statements = generate_create_neo4j_statement()
    run_neo4j_statements(neo4j_statements)
    
    neo4j_statements = generate_relationship_neo4j_statement()
    run_neo4j_statements(neo4j_statements)
        
if __name__ == "__main__":
    main()


