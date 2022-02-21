import pandas as pd

# source & destination paths
source_path = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/test_file.xlsx"
dest_path = "C:/Users/jithendra.kanna/Documents/accelerator_discussion/json_control_files/sample/"

#DDL structure
SAMPLE_RAW_DDL = """
CREATE OR REPLACE EXTERNAL TABLE RAW_<application_name>.<table_name>
(
<column_list>,
DH_ENTITY_TYPE VARCHAR AS (VALUE:DH_ENTITY_TYPE::VARCHAR),
DH_RECORD_LOAD_DATE TIMESTAMP_NTZ AS (VALUE:DH_RECORD_LOAD_DATE::TIMESTAMP_NTZ),
DH_BATCH_ID INT AS TO_NUMBER(SPLIT_PART(SPLIT_PART(METADATA$FILENAME, '/', 6),'=',2))
)
PARTITION BY (DH_BATCH_ID)
LOCATION = @COMMON_OBJECTS.S3_STAGE_RAW/<application_name>/<domain_name>/<small_table_name>/
FILE_FORMAT = COMMON_OBJECTS.PARQUET_FF;
"""

# dataframe from data dictionary
df = pd.read_excel(source_path)

# dataframe of unique table names with application, database, schema, domain
dd = df[['application', 'database', 'schema', 'domain', 'raw_table_name']].drop_duplicates()
print(dd)

for row in dd.itertuples(index=False):
    application = row[0]
    database = row[1]
    schema = row[2]
    domain = row[3]
    table_name = row[4]
    
    # dataframe for each table
    tbl_df = df.query('raw_table_name == "'+table_name+'"')
    column_list = tbl_df['raw_column_name'] + " " + tbl_df['raw_data_type'] +" AS (VALUE:" + tbl_df['raw_column_name']  + "::" + tbl_df['raw_data_type'] + ")"

    column_list = ','.join(column_list.values.tolist()).upper()
    
    final_ddl = SAMPLE_RAW_DDL.replace('<table_name>', table_name.upper()).replace('<column_list>', column_list).replace('<domain_name>',domain).replace('<small_table_name>', table_name).replace('<application_name>', application)
    print(final_ddl)
    
    # write DDL files in the destination
    with open(dest_path+table_name+".txt", "w") as f:
        f.write(final_ddl)


