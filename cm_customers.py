import pandas as pd
import os
from config import get_connection,get_output_path

def cm_customers():
    print("connenting to oracle")
    conn=get_connection()
    query="SELECT * FROM classicmodels.customers"
    df=pd.read_sql(query,conn)

    output_dir=get_output_path()
    output_file=os.path.join(output_dir,"customers.csv")
    df.to_csv(output_file,index=False)

    print(f" customers.csv downloaded successfully to : {output_file}")
    conn.close()

if __name__ == "__main__":
     
    cm_customers()
