from flask import Flask, render_template
import os
from snowflake.connector import connect, DictCursor
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import hashlib

from datetime import date

app = Flask(__name__)

#Set up your connection variables
PASSWORD = os.getenv('SNOWSQL_PWD')
WAREHOUSE= os.getenv('WAREHOUSE')
#Set up your Snowflake userid here
USER='JDEY'
DATABASE='PRODCATALOG'
SCHEMA='PUBLIC'
ACCOUNT='dtsquaredpartner.eu-west-1'
pd.options.display.width = 0

conn = connect(
                user=USER,
                password=PASSWORD,
                account=ACCOUNT,
                warehouse=WAREHOUSE,
                database=DATABASE,
                schema=SCHEMA,
                role="sysadmin"
                )

#This switches you to the appropriate role to create a table and to the correct snowflake database
conn.cursor().execute("USE DATABASE "+DATABASE)

def pop_hub_sat(tablename):
    hub_table = tablename+"_HUB"
    hub_hashcol = tablename+"_HASH"
    sat_table = tablename + "_SAT"
    sat_hashcol = "HASHDIFF"

    # Delete the contents of the hub table
    sql = "delete from "+hub_table
    conn.cursor().execute(sql)

    # Delete the contents of the hub table
    sql = "delete from " + sat_table
    conn.cursor().execute(sql)

    #Load in the data
    original = tablename+".csv" # <- Replace with your path.
    delimiter = "," # Replace if you're using a different delimiter.

    # Load in the table data in to a dataframe
    df = pd.read_csv(original, usecols=[0], sep = delimiter)
    print(df)
    business_key = df.columns[0]
    print(business_key)
    for i in df.index:
        hashkey = hashlib.sha256(df[business_key][i].encode('utf-8')).hexdigest()
        df.at[i,hub_hashcol] = hashkey
        df.at[i,"LOAD_DATE"] = date.today()
        df.at[i,"SOURCE"] = "Python"

    print(df)
    write_pandas(conn, df, "VEHICLE_HUB")

    # Reload in the table data in to a dataframe
    df = pd.read_csv(original, sep=delimiter)

    for index, row in df.iterrows():
        sat_cols=""
        hub_hashkey = hashlib.sha256(row[0].encode('utf-8')).hexdigest()
        for col in row[1:]:
            sat_cols=sat_cols+col+"+"
        hashdiff = hashlib.sha256(sat_cols.encode('utf-8')).hexdigest()
        df.at[index,"HASHDIFF"]=hashdiff
        df.at[index,hub_hashcol] = hub_hashkey
    df["LOAD_DATE"]=date.today()
    df["EFFECTIVE_FROM"]=date.today()
    df["EFFECTIVE_TO"]='9999-12-31'
    df["SOURCE"]="Python"

    print(df)
    write_pandas(conn, df, sat_table)


pop_hub_sat("VEHICLE")

