import argparse
import json
import logging
from pathlib import Path

import ollama
import pandas as pd
import pydantic
from tqdm import tqdm
import subprocess

from classes import LLMOutput

# Sets up formatting for Logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

# Calling the LLM with the case note, and schema using ollama
def send_message(case_note: str, patient_id: str):
    # check for invalid patient ID first
    if not patient_id or patient_id == "PUnknown":
        logger.error(f"Data Error: Invalid Patient ID '{patient_id}' provided. Skipping LLM call.")
        return "ERROR_INVALID_ID"
    
    system_prompt = (
        "You are a medical knowledge graph extractor. Your goal is COMPLETE extraction.\n"
        "1. For each node, you MUST extract all attributes (e.g., for Patient: name, age, gender, ethnicity)."
        f"The Patient ID for this extraction is {patient_id}. \n"
        "2. For each edge, you MUST populate the 'attributes' dictionary with relevant data "
        "like 'start_date', 'occurrence_date', 'dose', 'intensity', or 'status'.\n"
        "3. Use the provided clinical note to fill in every bracketed field in the schema.\n"
        "4. If a value is missing in the text, use 'Unknown' for strings or -1 for numbers, but do not omit the key. "
        "If value is redacted, use 'Redacted' for strings or -1 for numbers.\n"
        "5. If no lab results, return an empty list for lab results.\n"
    )
    try:
        response = ollama.chat(
            model="hf.co/unsloth/medgemma-27b-text-it-GGUF:Q3_K_S",
            format=LLMOutput.model_json_schema(),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": case_note},
            ],
            options={
                "num_ctx": 4096,
                "temperature": 0.1,
                "stop": ["<|end_of_text|>", "###"]
            }
        )
        return response

    except Exception as e:
        # Detect if the error is a timeout (usually manifest as a Connection or Read error)
        error_msg = str(e).lower()
        if "timeout" in error_msg or "deadline" in error_msg:
            logger.error(f"Time Error: LLM exceeded processing limit for Patient {patient_id}.")
            return "ERROR_TIMEOUT"
        else:
            logger.error(f"System Error for {patient_id}: {e}")
            return "ERROR_SYSTEM_FAIL"  
    

# Saves a checkpoint of the jobs dataframe to a CSV file
def save_checkpoint(df: pd.DataFrame, path: Path):
    df.to_csv(path, index=False)
    logger.info(f"Checkpoint saved to {path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--jobs', type=str, required=True, help='Path to jobs.csv')
    parser.add_argument('-c', '--checkpoint_every', type=int, default=5, help='Checkpoint frequency')
    args = parser.parse_args()

    jobs_path = Path(args.jobs)
    jobs = pd.read_csv(jobs_path)
    processed_since_ckpt = 0

    script_dir = Path(__file__).resolve().parent
    output_dir = script_dir / "graph_outputs"
    output_dir.mkdir(exist_ok=True)
    
    logger.info(f"Output directory set to: {output_dir}")

    with tqdm(total=len(jobs)) as t:
        for idx, row in jobs.iterrows():
            if row['jobstatus'] == 'completed':
                t.update(1)
                continue
            
            jobid = row.get('patient_id')
            if pd.isna(jobid) or jobid == "":
                # Try to extract patientID from filename
                jobid = Path(row['filepath']).stem.split('_')[1]
            
            derived_id = f"P{jobid}"
            t.write(f"Processing {derived_id} (Status: {row['jobstatus']})")

            try:
                case_note = Path(row['filepath']).read_text(encoding="utf-8")
                response = send_message(case_note, derived_id)

                if response == "ERROR_INVALID_ID":
                    jobs.loc[idx, "jobstatus"] = "failed_bad_id"
                    t.write(f"CRITICAL: Job {derived_id} skipped due to invalid ID format.")
                    continue

                if response == "ERROR_TIMEOUT":
                    jobs.loc[idx, "jobstatus"] = "failed_timeout"
                    t.write(f"WARNING: Job {derived_id} timed out (likely too long).")
                    continue
                
                if response == "ERROR_SYSTEM_FAIL" or response is None:
                    jobs.loc[idx, "jobstatus"] = "failed_system"
                    continue

                # Parse the content string from the Ollama response
                content = response['message']['content']
                validated_output = LLMOutput.model_validate_json(content)
                
                outfile = output_dir / f"patient_{jobid}_graph.json"
                outfile.write_text(validated_output.model_dump_json(indent=2))

                jobs.loc[idx, "jobstatus"] = "completed"
                jobs.loc[idx, "llm_output_full_path"] = str(outfile)
                jobs.loc[idx, "last_updated"] = pd.Timestamp.now()

            except Exception as e:
                logger.error(f"Validation/System Error for {derived_id}: {e}")
                jobs.loc[idx, "jobstatus"] = "failed_parse"
            
            finally:
                processed_since_ckpt += 1
                t.update(1)
                if processed_since_ckpt >= args.checkpoint_every:
                    save_checkpoint(jobs, jobs_path)
                    processed_since_ckpt = 0

    save_checkpoint(jobs, jobs_path)
    logger.info("Batch processing complete.")

if __name__ == "__main__":
    main()