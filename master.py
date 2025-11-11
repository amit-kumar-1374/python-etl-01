import subprocess
import sys
from utils import get_new_batch, end_batch

# ==========================
# List of ETL scripts (sequential)
# ==========================
etl_scripts = [
    "source_to_s3/main.py",
  
    "s3_to_devstage/main.py",
    "devstage_to_devdw/main.py"
]

if __name__ == "__main__":
    # Get new batch from Redshift
    try:
        print("batch_control_log updates started")
        batch_no, batch_date = get_new_batch()
    except Exception:
        print("ETL batch cannot start. Check Redshift connection.")
        sys.exit(1)

    all_success = True

    # Run each ETL script sequentially
    for script in etl_scripts:
        print(f"\n[RUNNING] {script}")
        result = subprocess.run([sys.executable, script])
        if result.returncode != 0:
            print(f"[FAILED] {script}")
            all_success = False
            break
        else:
            print(f"[SUCCESS] {script}")

    # End batch
    status = "C" if all_success else "F"
    try:
        end_batch(batch_no, status)
    except Exception:
        print("Failed to update batch status. Check Redshift connection.")

    print(f"\nETL batch {batch_no} finished with status: {status}")
