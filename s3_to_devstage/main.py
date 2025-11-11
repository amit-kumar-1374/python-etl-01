import subprocess
from dotenv import load_dotenv
import os
import sys

load_dotenv()

# Get current folder of this script
base_dir = os.path.dirname(os.path.abspath(__file__))

# Load scripts and make full paths relative to this folder
scripts = os.getenv("scripts", "").split(",")
scripts = [os.path.join(base_dir, s.strip()) for s in scripts]

# ==========================
# Status functions
# ==========================
def mark_start(script_name):
    print(f"\n[RUNNING] {script_name}")

def mark_success(script_name):
    print(f"[SUCCESS] {script_name}")

def mark_failed(script_name, error=None):
    print(f"[FAILED] {script_name}")
    if error:
        print(f"Error: {error}")

# ==========================
# Main function
# ==========================
def main():
    if not scripts:
        print("No scripts defined in .env file")
        return

    for script in scripts:
        mark_start(script)
        if not os.path.exists(script):
            mark_failed(script, "Script not found")
            raise FileNotFoundError(f"{script} not found")
        try:
            result = subprocess.run([sys.executable, script],
                                    check=True, capture_output=True, text=True)
            print(result.stdout)
            mark_success(script)
        except subprocess.CalledProcessError as e:
            mark_failed(script, e.stderr)
            raise RuntimeError(f"{script} failed") from e

    print("\nAll source_to_s3 scripts completed successfully!")

if __name__ == "__main__":
    main()
