import subprocess

scripts = [
    "cm_customers.py",
    "cm_employees.py",
    "cm_offices.py",
    "cm_orders.py",
    "cm_orderdetails.py",
    "cm_payments.py",
    "cm_products.py",
    "cm_productlines.py"
]

def main():
    for script in scripts:
        print(f" Running {script} ...")
        subprocess.run(["python", script])
    print("\n All tables downloaded successfully!")

if __name__ == "__main__":
    main()
