import pandas as pd
import random

# 1. Setup Patient Data (10 personas)
patients_data = [
    {"id": "S1001", "name": "Tan Wei Ling", "age": 72, "gender": "Female", "has_rdm": True},
    {"id": "S1002", "name": "Muhammad Syazwan", "age": 29, "gender": "Male", "has_rdm": False},
    {"id": "S1003", "name": "Rajendran s/o Muthu", "age": 58, "gender": "Male", "has_rdm": True},
    {"id": "S1004", "name": "Chloe de Souza", "age": 41, "gender": "Female", "has_rdm": False},
    {"id": "S1005", "name": "Lim Boon Hock", "age": 84, "gender": "Male", "has_rdm": False},
    {"id": "S1006", "name": "Fatimah Binte Ahmad", "age": 65, "gender": "Female", "has_rdm": True},
    {"id": "S1007", "name": "David Tan", "age": 35, "gender": "Male", "has_rdm": False},
    {"id": "S1008", "name": "Priya Devi", "age": 52, "gender": "Female", "has_rdm": True},
    {"id": "S1009", "name": "Jeremy Wong", "age": 47, "gender": "Male", "has_rdm": False},
    {"id": "S1010", "name": "Siti Aminah", "age": 78, "gender": "Female", "has_rdm": False}
]

# RDM Entities
rdm_triggers = ["Crush Injury", "Statin-Induced Myopathy", "Extreme Physical Exertion", "Prolonged Immobilization"]
rdm_complications = ["Acute Kidney Injury", "Hyperkalemia", "Compartment Syndrome", "Metabolic Acidosis"]
rdm_meds = ["0.9% Normal Saline (IV)", "Sodium Bicarbonate", "Mannitol", "Calcium Gluconate"]
general_conditions = ["Hypertension", "Type 2 Diabetes", "Hyperlipidemia", "Atorvastatin Use"]

nodes, edges, unique_entities = [], [], {}

def add_node(id, label, props):
    nodes.append({"id": id, "label": label, **props})

def add_edge(src, tgt, type, props=None):
    edges.append({"source": src, "target": tgt, "type": type, **(props or {})})

for p in patients_data:
    pid = p['id']
    add_node(pid, "Patient", {"name": p['name'], "age": p['age'], "gender": p['gender']})
    
    # Pre-existing conditions
    p_conds = random.sample(general_conditions, 2)
    for cond in p_conds:
        cid = f"Cond_{cond.replace(' ', '_')}"
        if cid not in unique_entities:
            add_node(cid, "Condition", {"name": cond})
            unique_entities[cid] = True
        add_edge(pid, cid, "HAS_HISTORY")

    # If the patient has Rhabdomyolysis (RDM)
    if p['has_rdm']:
        trigger = random.choice(rdm_triggers)
        complication = random.choice(rdm_complications)
        treatment = random.choice(rdm_meds)
        
        # Trigger Node
        tid = f"Trigger_{trigger.replace(' ', '_')}"
        if tid not in unique_entities:
            add_node(tid, "Trigger", {"name": trigger})
            unique_entities[tid] = True
        add_edge(pid, tid, "TRIGGERED_BY")
        
        # Complication Node
        coid = f"Comp_{complication.replace(' ', '_')}"
        if coid not in unique_entities:
            add_node(coid, "Complication", {"name": complication})
            unique_entities[coid] = True
        add_edge(pid, coid, "DEVELOPED")
        
        # Treatment Node
        mid = f"Med_{treatment.replace(' ', '_')}"
        if mid not in unique_entities:
            add_node(mid, "Treatment", {"name": treatment})
            unique_entities[mid] = True
        add_edge(pid, mid, "TREATED_WITH")

    # Generate Note "Bridges" for the clinical journey
    for i in range(1, 6): # Reduced to 5 notes for clarity
        nid = f"Note_{pid}_{i}"
        add_node(nid, "Note", {"type": "Clinical Summary", "visit": i})
        add_edge(pid, nid, "HAS_NOTE")
        if p['has_rdm']:
            # In clinical notes for RDM, we track CK levels
            ck_level = random.randint(1000, 50000)
            add_edge(nid, tid, "DOCUMENTS_ETIOLOGY", {"ck_level": ck_level})
            add_edge(nid, mid, "DOCUMENTS_THERAPY")

# Save
pd.DataFrame(nodes).to_csv('kg_nodes_rdm.csv', index=False)
pd.DataFrame(edges).to_csv('kg_edges_rdm.csv', index=False)