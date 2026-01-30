from pydantic import BaseModel, Field
from typing import List, Optional

# Node Models
class PatientNode(BaseModel):
    id: str = Field(description="Unique patient identifier provided in the system prompt, e.g., 'P99'")
    name: str = Field(description="Full legal name of the patient"  )
    age: int = Field(description="Numerical age; use -1 if not mentioned")
    gender: str = Field(description="Gender; choose from [Male, Female]")
    ethnicity: str = Field(description="Patient ethnicity, e.g., 'Chinese', 'Malay', 'Indian'")

class MedicationNode(BaseModel):
    id: str = Field(description="Generic or Brand name used as a unique ID, e.g., 'Metformin'")
    name: str = Field(description="The full name of the medication can be found in the text, e.g., 'Metformin'")

class ConditionNode(BaseModel):
    id: str = Field(description="Short clinical code or name, e.g., 'T2DM', 'Hypertension'")
    name: str = Field(description="The full clinical name of the diagnosis can be found in the text, e.g., 'Type 2 Diabetes Mellitus'")

class EncounterNode(BaseModel):
    id: str = Field(description="Unique identifier for the hospital visit, e.g., 'E9901'")
    type: str = Field(description="Setting of care: e.g., 'Inpatient', 'Outpatient', 'ER'")

class LabResultNode(BaseModel):
    id: str = Field(description="Unique ID for this specific test result, e.g., 'L1'")
    test_name: str = Field(description="Name of the lab test, e.g., 'HbA1c', 'Serum Creatinine'")
    value: float = Field(description="Numerical result only. If the text says '8.2%', enter 8.2")
    unit: str = Field(description="Measurement unit, e.g., '%', 'umol/L', 'mg/dL'")

# Edge Models 
class PrescribedEdge(BaseModel):
    patient_id: str = Field(description="Must match the PatientNode ID")
    medication_id: str = Field(description="Must match the MedicationNode ID")
    start_date: str = Field(description="Date medication was started (YYYY-MM-DD)")
    end_date: str = Field(default="Ongoing", description="Date ended or 'Ongoing'")
    dose: str = Field(description="Dosage amount, e.g., '500mg' or '20 units'")
    intensity: str = Field(description="Frequency/Schedule, e.g., 'Once daily', 'BD', 'TDS'")

class HasConditionEdge(BaseModel):
    patient_id: str = Field(description="Must match the PatientNode ID")
    condition_id: str = Field(description="Must match the ConditionNode ID")
    occurrence_date: str = Field(description="Date of diagnosis or onset (YYYY-MM-DD)")
    status: str = Field(description="Clinical status: 'Active', 'Resolved', or 'Chronic'")

class HadEncounterEdge(BaseModel):
    patient_id: str = Field(description="Must match the PatientNode ID")
    encounter_id: str = Field(description="Must match the EncounterNode ID")
    occurrence_date: str = Field(description="Date of admission/visit (YYYY-MM-DD)")

class ResultedInEdge(BaseModel):
    encounter_id: str = Field(description="Must match the EncounterNode ID")
    lab_id: str = Field(description="Must match the LabResultNode ID")

# Final Container Model
class LLMOutput(BaseModel):
    """
    Extracted Clinical Knowledge Graph. 
    Ensure every list is populated based on the provided clinical note.
    """
    patient_nodes: List[PatientNode] = Field(description="Exactly one patient node per summary")
    medication_nodes: List[MedicationNode] = Field(description="All unique drugs mentioned")
    condition_nodes: List[ConditionNode] = Field(description="All unique diagnoses mentioned")
    encounter_nodes: List[EncounterNode] = Field(description="The hospital visit details")
    lab_nodes: List[LabResultNode] = Field(description="Specific lab values and measurements")
    
    prescribed_edges: List[PrescribedEdge] = Field(description="Links patient to their medications")
    condition_edges: List[HasConditionEdge] = Field(description="Links patient to their conditions")
    encounter_edges: List[HadEncounterEdge] = Field(description="Links patient to the hospital visit")
    lab_result_edges: List[ResultedInEdge] = Field(description="Links the encounter to specific lab results")
    
    confidence: int = Field(ge=1, le=7, description="Confidence score for the extraction")
    justification: str = Field(description="Brief medical reasoning for these specific extractions")