import pandas as pd
import os
from config import get_connection,get_output_path

def cm_offices():
    print("connenting to oracle")
    conn=get_connection()
    query="SELECT * FROM classicmodels.offices"
    df=pd.read_sql(query,conn)

    output_dir=get_output_path()
    output_file=os.path.join(output_dir,"offices.csv")
    df.to_csv(output_file,index=False)

    print(f" offices.csv downloaded successfully to : {output_file}")
    conn.close()

if __name__ == "__main__":
     
    cm_offices()

