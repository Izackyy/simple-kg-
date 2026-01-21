import pandas as pd
import random

# 1. Patient Persona Setup (10 patients)
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

# RDM Clinical Libraries
rdm_triggers = ["Crush Injury", "Statin-Induced Myopathy", "Extreme Physical Exertion", "Prolonged Immobilization"]
rdm_complications = ["Acute Kidney Injury", "Hyperkalemia", "Compartment Syndrome", "Metabolic Acidosis"]
rdm_meds = ["0.9% Normal Saline (IV)", "Sodium Bicarbonate", "Mannitol", "Calcium Gluconate"]

nodes, edges, unique_entities = [], [], {}

def add_node(id, label, props):
    nodes.append({"id": id, "label": label, **props})

def add_edge(src, tgt, type, props=None):
    edges.append({"source": src, "target": tgt, "type": type, **(props or {})})

# 2. Generation Loop
for p in patients_data:
    pid = p['id']
    # Create Patient Node
    add_node(pid, "Patient", {"name": p['name'], "age": p['age'], "gender": p['gender'], "has_rdm": p['has_rdm']})
    
    if p['has_rdm']:
        idx = patients_data.index(p) % 4
        trigger, comp, med = rdm_triggers[idx], rdm_complications[idx], rdm_meds[idx]
        
        # 1. Trigger Node & Edge (Carrying CK Level)
        tid = f"Trigger_{trigger.replace(' ', '_')}"
        if tid not in unique_entities:
            add_node(tid, "Trigger", {"name": trigger})
            unique_entities[tid] = True
        add_edge(pid, tid, "TRIGGERED_BY", {"peak_ck_level": random.randint(5000, 50000)})
        
        # 2. Complication Node & Edge
        coid = f"Comp_{comp.replace(' ', '_')}"
        if coid not in unique_entities:
            add_node(coid, "Complication", {"name": comp})
            unique_entities[coid] = True
        add_edge(pid, coid, "DEVELOPED")
        
        # 3. Treatment Node & Edge
        mid = f"Med_{med.replace(' ', '_')}"
        if mid not in unique_entities:
            add_node(mid, "Treatment", {"name": med})
            unique_entities[mid] = True
        add_edge(pid, mid, "TREATED_WITH")

# 3. Export to CSV
pd.DataFrame(nodes).to_csv('nodes_direct_rdm.csv', index=False)
pd.DataFrame(edges).to_csv('edges_direct_rdm.csv', index=False)
print("Files generated: nodes_direct_rdm.csv, edges_direct_rdm.csv")