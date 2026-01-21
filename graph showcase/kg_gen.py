import pandas as pd
import random

# 1. Patient Data Setup
patients_data = [
    {"id": "S1001", "name": "Tan Wei Ling", "age": 72, "gender": "Female", "race": "Chinese", "religion": "Buddhist", "has_rdm": True},
    {"id": "S1002", "name": "Muhammad Syazwan", "age": 29, "gender": "Male", "race": "Malay", "religion": "Muslim", "has_rdm": False},
    {"id": "S1003", "name": "Rajendran s/o Muthu", "age": 58, "gender": "Male", "race": "Indian", "religion": "Hindu", "has_rdm": True},
    {"id": "S1004", "name": "Chloe de Souza", "age": 41, "gender": "Female", "race": "Eurasian", "religion": "Christian", "has_rdm": False},
    {"id": "S1005", "name": "Lim Boon Hock", "age": 84, "gender": "Male", "race": "Chinese", "religion": "Taoist", "has_rdm": False},
    {"id": "S1006", "name": "Fatimah Binte Ahmad", "age": 65, "gender": "Female", "race": "Malay", "religion": "Muslim", "has_rdm": True},
    {"id": "S1007", "name": "David Tan", "age": 35, "gender": "Male", "race": "Chinese", "religion": "Christian", "has_rdm": False},
    {"id": "S1008", "name": "Priya Devi", "age": 52, "gender": "Female", "race": "Indian", "religion": "Hindu", "has_rdm": True},
    {"id": "S1009", "name": "Jeremy Wong", "age": 47, "gender": "Male", "race": "Chinese", "religion": "Buddhist", "has_rdm": False},
    {"id": "S1010", "name": "Siti Aminah", "age": 78, "gender": "Female", "race": "Malay", "religion": "Muslim", "has_rdm": False}
]

# Clinical Content
rdm_triggers = ["Crush Injury", "Statin-Induced Myopathy", "Extreme Physical Exertion", "Prolonged Immobilization"]
rdm_complications = ["Acute Kidney Injury", "Hyperkalemia", "Compartment Syndrome", "Metabolic Acidosis"]
rdm_meds = ["0.9% Normal Saline (IV)", "Sodium Bicarbonate", "Mannitol", "Calcium Gluconate"]

nodes, edges, unique_entities = [], [], {}

def add_node(id, label, props):
    nodes.append({"id": id, "label": label, **props})

def add_edge(src, tgt, type, props=None):
    edges.append({"source": src, "target": tgt, "type": type, **(props or {})})

# 2. Loop Through Patients
for p in patients_data:
    pid = p['id']
    add_node(pid, "Patient", {"name": p['name'], "age": p['age'], "gender": p['gender'], "has_rdm": p['has_rdm']})
    
    # Add Demographic Nodes & Edges
    for label, val in [("Race", p['race']), ("Religion", p['religion']), ("AgeGroup", f"{(p['age'] // 10) * 10}s")]:
        eid = f"{label}_{val.replace(' ', '_')}"
        if eid not in unique_entities:
            add_node(eid, label, {"name": val})
            unique_entities[eid] = True
        add_edge(pid, eid, f"HAS_{label.upper()}")

    # Add Clinical Connections for RDM
    if p['has_rdm']:
        idx = patients_data.index(p) % 4
        trigger, comp, med = rdm_triggers[idx], rdm_complications[idx], rdm_meds[idx]
        
        # Trigger
        tid = f"Trigger_{trigger.replace(' ', '_')}"
        if tid not in unique_entities:
            add_node(tid, "Trigger", {"name": trigger})
            unique_entities[tid] = True
        add_edge(pid, tid, "TRIGGERED_BY", {"peak_ck_level": random.randint(5000, 50000)})
        
        # Complication
        coid = f"Comp_{comp.replace(' ', '_')}"
        if coid not in unique_entities:
            add_node(coid, "Complication", {"name": comp})
            unique_entities[coid] = True
        add_edge(pid, coid, "DEVELOPED")
        
        # Treatment
        mid = f"Med_{med.replace(' ', '_')}"
        if mid not in unique_entities:
            add_node(mid, "Treatment", {"name": med})
            unique_entities[mid] = True
        add_edge(pid, mid, "TREATED_WITH")

# 3. Export
pd.DataFrame(nodes).to_csv('nodes_rdm_demographics.csv', index=False)
pd.DataFrame(edges).to_csv('edges_rdm_demographics.csv', index=False)