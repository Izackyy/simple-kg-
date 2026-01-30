import json
import logging
import sys
from pathlib import Path
from neo4j import GraphDatabase

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class Neo4jIngester:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        with self.driver.session() as session:
            res = session.run("RETURN 'Connection Successful' as msg")
            logger.info(f"Neo4j Status: {res.single()['msg']} at {uri}")

    def close(self):
        self.driver.close()

    def ingest_fragment(self, file_path: Path):
        with open(file_path, 'r') as f:
            data = json.load(f)

        with self.driver.session() as session:
            node_mappings = {
                "patient_nodes": "Patient",
                "medication_nodes": "Medication",
                "condition_nodes": "Condition",
                "encounter_nodes": "Encounter",
                "lab_nodes": "LabResult"
            }
            for list_key, label in node_mappings.items():
                for node_data in data.get(list_key, []):
                    session.execute_write(self._merge_node, label, node_data)

            edge_mappings = [
                ("prescribed_edges", "PRESCRIBED", "Patient", "Medication", "patient_id", "medication_id"),
                ("condition_edges", "HAS_CONDITION", "Patient", "Condition", "patient_id", "condition_id"),
                ("encounter_edges", "HAD_ENCOUNTER", "Patient", "Encounter", "patient_id", "encounter_id"),
                ("lab_result_edges", "RESULTED_IN", "Encounter", "LabResult", "encounter_id", "lab_id")
            ]
            for list_key, rel_type, src_label, tgt_label, src_key, tgt_key in edge_mappings:
                for edge_data in data.get(list_key, []):
                    session.execute_write(self._merge_edge, rel_type, src_label, tgt_label, src_key, tgt_key, edge_data)

    @staticmethod
    def _merge_node(tx, label, props):
        query = (
            f"MERGE (n:{label} {{id: $props.id}}) "
            f"SET n += $props "
            f"SET n.start_date = CASE WHEN n.start_date =~ '\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN date(n.start_date) ELSE n.start_date END "
            f"SET n.end_date = CASE WHEN n.end_date =~ '\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN date(n.end_date) ELSE n.end_date END "
            f"RETURN n"
        )
        tx.run(query, props=props)

    @staticmethod
    def _merge_edge(tx, rel_type, src_label, tgt_label, src_key, tgt_key, props):
        source_id = props.pop(src_key)
        target_id = props.pop(tgt_key)
        query = (
            f"CYPHER runtime=pipelined "
            f"MATCH (a:{src_label} {{id: $source_id}}) "
            f"WITH a "
            f"MATCH (b:{tgt_label} {{id: $target_id}}) "
            f"MERGE (a)-[r:{rel_type}]->(b) "
            f"SET r += $props "
            f"SET r.start_date = CASE WHEN r.start_date =~ '\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN date(r.start_date) ELSE r.start_date END "
            f"SET r.occurrence_date = CASE WHEN r.occurrence_date =~ '\\d{{4}}-\\d{{2}}-\\d{{2}}' THEN date(r.occurrence_date) ELSE r.occurrence_date END "
            f"RETURN r"
        )
        tx.run(query, source_id=source_id, target_id=target_id, props=props)

def main():
    script_dir = Path(__file__).resolve().parent
    output_dir = script_dir / "graph_outputs"
    
    logger.info(f"Searching for JSON in: {output_dir.absolute()}")
    
    # Neo4j connection parameters change if needed
    port = sys.argv[1] if len(sys.argv) > 1 else "7687"
    URI = f"neo4j://127.0.0.1:{port}"
    USER = "neo4j"
    PASSWORD = "Password1!" 

    if not output_dir.exists():
        logger.error(f"Error: Folder NOT found at {output_dir.absolute()}")
        return

    json_files = list(output_dir.glob("*.json"))
    logger.info(f"Found {len(json_files)} files: {[f.name for f in json_files]}")

    if not json_files:
        return

    ingester = Neo4jIngester(URI, USER, PASSWORD)
    try:
        for json_file in json_files:
            logger.info(f"Ingesting {json_file.name}...")
            ingester.ingest_fragment(json_file)
        logger.info("Ingestion complete.")
    finally:
        ingester.close()

if __name__ == "__main__":
    main()