import subprocess
from dotenv import load_dotenv
import os
import sys

load_dotenv()

# Get folder of this script
base_dir = os.path.dirname(os.path.abspath(__file__))

# Load scripts from .env and make full paths
all_scripts = os.getenv("all_scripts", "").split(",")
all_scripts = [os.path.join(base_dir, s.strip()) for s in all_scripts]

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
    if not all_scripts:
        print("No scripts defined in .env")
        return

    for script in all_scripts:
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

    print("\nAll tables downloaded successfully!")

if __name__ == "__main__":
    main()
