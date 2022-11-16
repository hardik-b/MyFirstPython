import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, date, time, timedelta


# Third-party library
import pandas as pd
import boto3
import botocore
import awswrangler as wr
import pytz

LOCAL_TZ = pytz.timezone("Australia/Sydney")
DT_FORMAT = "%d_%m_%Y_%H_%M_%S"

def directJDBCSource(
    glueContext,
    connectionName,
    connectionType,
    database,
    table,
    redshiftTmpDir,
    transformation_ctx,
) -> DynamicFrame:

    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": table,
        "connectionName": connectionName,
    }

    if redshiftTmpDir:
        connection_options["redshiftTmpDir"] = redshiftTmpDir

    return glueContext.create_dynamic_frame.from_options(
        connection_type=connectionType,
        connection_options=connection_options,
        transformation_ctx=transformation_ctx,
    )


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Oracle SQL table
# OracleSQLtable_node1 = directJDBCSource(
    # glueContext,
    # connectionName="test-cr5505-vots-db",
    # connectionType="oracle",
    # database="votsdev",
    # table="",
    # redshiftTmpDir="",
    # transformation_ctx="OracleSQLtable_node1",
# )

today = datetime.now(LOCAL_TZ)
print("Today:", today)

print("CR5505: Trying to connect to DB")
#secret_str = """{ "host":"vots-int-rds-19c.cz89abdku6n8.ap-southeast-2.rds.amazonaws.com", "username":"vots_owner", "password":"votsOwner#Int", "engine":"oracle", "port":"1521", db_name="VOTSINT"}"""
con = wr.oracle.connect(connection="test-cr5505-vots-db")
#con = wr.oracle.connect(secret_id="""{ "host":"vots-int-rds-19c.cz89abdku6n8.ap-southeast-2.rds.amazonaws.com", "username":"vots_owner", "password":"votsOwner#Int", "engine":"oracle", "port":"1521", db_name="VOTSINT"}""")
print("CR5505: Trying to RUN SQL query")
df = wr.oracle.read_sql_query(
    sql="select * from VTDLG_WORK_QUEUE where dlng_no in ('AS619290K')",
    con=con
)

df1 = wr.oracle.read_sql_query(
    sql="select * from VTDLG_WORK_QUEUE where dlng_no in ('AS619290K')",
    con=con
)

#df.merge(df1, how='outer')
df2 = pd.concat([df, df1])

fileName = "vots_dealings_" + datetime.strftime(today, DT_FORMAT) + ".csv"
#path1 = f"s3://vots-int-build-packages/Misc/cr5505/testfile1.csv"
path1 = f"s3://vots-int-build-packages/Misc/cr5505/{fileName}"

print("CR5505: Trying to write CSV")
wr.s3.to_csv(df2, path1, index=False)
con.close()
job.commit()
