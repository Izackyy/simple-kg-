import pandas as pd
import re
import logging
from pathlib import Path
from datetime import datetime

# Logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

def create_job_queue(cases_folder: str, output_csv: str = "jobs.csv"):
    folder_path = Path(cases_folder)
    if not folder_path.exists():
        logger.error(f"Folder {cases_folder} does not exist.")
        return

    # Get current files on disk
    current_files = list(folder_path.glob("*.txt"))
    current_filepaths = [str(f.absolute()) for f in current_files]
    
    new_jobs = []
    for note_file in current_files:
        match = re.search(r'File(\d+)', note_file.name)
        patient_id = match.group(1) if match else note_file.stem
        
        new_jobs.append({
            "patient_id": patient_id,
            "filepath": str(note_file.absolute()),
            "jobstatus": "pending",
            "llm_output_full_path": "",
            "last_updated": None,
            "date_created": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    new_df = pd.DataFrame(new_jobs)

    # Sync with existing jobs.csv if it exists
    if Path(output_csv).exists():
        existing_df = pd.read_csv(output_csv)
        
        # Filter out rows where the file no longer exists on disk
        initial_count = len(existing_df)
        existing_df = existing_df[existing_df['filepath'].isin(current_filepaths)]
        removed_count = initial_count - len(existing_df)
        
        if removed_count > 0:
            logger.info(f"Removed {removed_count} orphaned jobs (files no longer in folder).")

        # Only add files from new_df that are NOT already in existing_df
        df = pd.concat([
            existing_df, 
            new_df[~new_df['filepath'].isin(existing_df['filepath'])]
        ], ignore_index=True)
    else:
        df = new_df

    df.to_csv(output_csv, index=False)
    logger.info(f"Final queue: {len(df)} jobs saved to {output_csv}.")
if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    DATA_FOLDER = script_dir.parent / "data" / "SyntheticHSATestData"
    if not DATA_FOLDER.exists():
        print(f"Error: Path not found at {DATA_FOLDER}")
    else:
        print(f"Path identified: {DATA_FOLDER}")
        create_job_queue(str(DATA_FOLDER))