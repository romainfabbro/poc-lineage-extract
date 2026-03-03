import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

from databricks.sdk import WorkspaceClient
from neo4j import GraphDatabase
from dotenv import load_dotenv

# This will load the .env file if it's not already in the environment
load_dotenv() 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LineageIngestor:
    def __init__(self, profile_name: str | None = None, catalog: str = "main"):
        # Initialize Client using CLI credentials
        self.db_client = WorkspaceClient(profile=profile_name)
        self.catalog = catalog

        # Local Neo4j Connection
        neo4j_pwd = os.getenv("NEO4J_PASSWORD", "")
        neo4j_user = os.getenv("NEO4J_USER", "")
        self.neo_driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=(neo4j_user, neo4j_pwd)
        )

    def get_all_tables(self) -> List[str]:
        """
        Retrieves all MANAGED and EXTERNAL tables from the catalog using SQL.
        Requires a running SQL Warehouse.
        """
        logging.info(f"Listing tables in catalog '{self.catalog}'...")
        
        # Get a Warehouse ID (Auto-detect the first available one)
        warehouses = list(self.db_client.warehouses.list())
        if not warehouses:
            raise ValueError("No SQL Warehouses found. Please create or start one.")
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
        logging.info(f"Using Warehouse: {warehouses[0].name} ({warehouse_id})")

        query = """
        SELECT table_catalog || '.' || table_schema || '.' || table_name as full_name
        FROM system.information_schema.tables 
        WHERE table_catalog like 'adp_dll_acc%'
          AND table_type IN ('MANAGED', 'EXTERNAL')
        """
        
        try:
            resp = self.db_client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=warehouse_id,
                wait_timeout="50s"
            )
            
            if resp.result and resp.result.data_array:
                return [row.values[0] for row in resp.result.data_array]
            return []
        except Exception as e:
            logging.error(f"Failed to list tables: {e}")
            return []


    def get_active_warehouse_id(self) -> str:
        logging.info("Searching for a Serverless SQL Warehouse...")
        
        try:
            warehouses = list(self.db_client.warehouses.list())
            
            # We check warehouse_type. Value is usually WarehouseType.SERVERLESS
            serverless_warehouses = [
                wh for wh in warehouses 
                if wh.warehouse_type and wh.warehouse_type.value == "SERVERLESS"
            ]
            
            if serverless_warehouses:
                # Sort to prioritize RUNNING ones if multiple exist
                serverless_warehouses.sort(key=lambda x: x.state.value == "RUNNING", reverse=True)
                selected = serverless_warehouses[0]
                logging.info(f"✅ Found Serverless Warehouse: {selected.name} (ID: {selected.id})")
                return selected.id
            
            logging.warning("No Serverless warehouse found. Falling back to first available.")
            if warehouses:
                return warehouses[0].id
                
        except Exception as e:
            logging.error(f"Error searching for warehouses: {e}")

        raise ValueError("CRITICAL: No SQL Warehouse (Serverless or otherwise) could be found.")

    def get_tables_in_catalog(self, catalog: str, warehouse_id: str) -> List[str]:
        if not warehouse_id:
            return []

        query = f"SELECT table_catalog || '.' || table_schema || '.' || table_name FROM {catalog}.information_schema.tables WHERE table_type IN ('MANAGED', 'EXTERNAL')"
        
        try:
            resp = self.db_client.statement_execution.execute_statement(
                statement=query,
                warehouse_id=warehouse_id,
                wait_timeout="30s"
            )
            return [row[0] for row in resp.result.data_array] if resp.result.data_array else []
        except Exception as e:
            logging.warning(f"Skipping {catalog}: {e}")
            return []

    def fetch_lineage_rest(self, table_name: str) -> List[Dict]:
            edges = []
            path = "/api/2.0/lineage-tracking/table-lineage"
            params = {"table_name": table_name, "include_entity_lineage": "true"}

            try:
                response = self.db_client.api_client.do("GET", path, query=params)
                
                upstreams = response.get("upstreams", [])
                if upstreams:
                    logging.info(f"✅ Found {len(upstreams)} parents for {table_name}")
                
                for source in upstreams:
                    if "tableInfo" in source and source["tableInfo"]:
                        edges.append({
                            "source": source["tableInfo"]["name"],
                            "source_catalog": source["tableInfo"]["name"].split('.')[0],
                            "target": table_name,
                            "target_catalog": table_name.split('.')[0]
                        })
            except Exception as e:
                pass
            return edges


    def get_mesh_catalogs(self):
        return ["adp_dll_acc_clean", "adp_dll_acc_prep"]
        
    def write_batch_to_neo4j(self, edges: List[Dict]):
        if not edges:
            return

        # Cypher query using UNWIND for high-performance batch insertion
        query = """
        UNWIND $batch AS row
        MERGE (s:Table {name: row.source})
        MERGE (t:Table {name: row.target})
        MERGE (s)-[:FEEDS_INTO]->(t)
        """
        
        try:
            with self.neo_driver.session() as session:
                session.run(query, batch=edges)
        except Exception as e:
            logging.error(f"Neo4j Write Error: {e}")

    def run(self):
        logging.info("--- Starting Diagnosis ---")
        
        with self.neo_driver.session() as session:
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Table) REQUIRE t.name IS UNIQUE")
            logging.info("✅ Neo4j: Connection and Constraints verified.")

        wh_id = self.get_active_warehouse_id()
        catalogs = self.get_mesh_catalogs()
        logging.info(f"Checking catalogs: {catalogs}")
        
        all_tables = []
        for cat in catalogs:
            tables = self.get_tables_in_catalog(cat, wh_id)
            logging.info(f"🔍 Catalog '{cat}': Found {len(tables)} tables.")
            all_tables.extend(tables)

        if not all_tables:
            logging.error("❌ No tables found! Is your CATALOG_SCOPE correct?")
            return

        logging.info(f"Total Tables to analyze: {len(all_tables)}")

        all_edges = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_table = {executor.submit(self.fetch_lineage_rest, t): t for t in all_tables}
            
            processed_count = 0
            for future in as_completed(future_to_table):
                processed_count += 1
                edges = future.result()
                if edges:
                    all_edges.extend(edges)
                
                if processed_count % 50 == 0:
                    logging.info(f"Progress: {processed_count}/{len(all_tables)} tables checked. Found {len(all_edges)} relationships so far.")

        if all_edges:
            logging.info(f"🚀 Writing {len(all_edges)} relationships to Neo4j...")
            self.write_batch_to_neo4j(all_edges)
            logging.info("✅ Ingestion Complete.")
        else:
            logging.error("❌ Extraction complete, but ZERO relationships were found in Unity Catalog.")
            logging.warning("Note: Lineage only appears if tables have been JOINED or CREATED via SQL/Dataframes recently.")

        self.neo_driver.close()

if __name__ == "__main__":
    # If you have a specific profile in .databrickscfg, use profile_name="my-profile"
    ingestor = LineageIngestor(profile_name=os.getenv("DATABRICKS_PROFILE", ""))
    ingestor.run()