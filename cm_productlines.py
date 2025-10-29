import pandas as pd
import os
from config import get_connection,get_output_path

def cm_productlines():
    print("connenting to oracle")
    conn=get_connection()
    query="SELECT * FROM classicmodels.productlines"
    df=pd.read_sql(query,conn)

    output_dir=get_output_path()
    output_file=os.path.join(output_dir,"productlines.csv")
    df.to_csv(output_file,index=False)

    print(f" productlines.csv downloaded successfully to : {output_file}")
    conn.close()

if __name__ == "__main__":
     
    cm_productlines()
