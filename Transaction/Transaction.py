import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Configuration
s3_bucket = 'bankbucket01'
s3_key = 'inbound_csv/bank.csv'  # Corrected path
rds_host = 'sql2022.cbqsayssgsid.ap-south-1.rds.amazonaws.com'
rds_port = '1433'  # SQL Server port
rds_database = 'sql2022'
rds_user = 'admin'
rds_password = 'admin1000'
jdbc_url = f"jdbc:sqlserver://{rds_host}:{rds_port};databaseName={rds_database}"
print(jdbc_url)
#database.cbqsayssgsid.ap-south-1.rds.amazonaws.com
# Load CSV from S3
df = spark.read.option("header", "true").csv(f"s3://{s3_bucket}/{s3_key}")

# Write to RDS
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "bank_csv") \
    .option("user", rds_user) \
    .option("password", rds_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("append") \
    .save()

print("Data loaded into the banking table successfully.")
