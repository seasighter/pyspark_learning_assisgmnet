# should read file from hdfs
# currently accessign locally due to unavailibilty of gcp account

# read file from locally

from session_builder import spark_session
from schema_config import schema

# reading file from local
def read_file(file_path):
    try:
        health_data = spark_session.read.format("csv")\
        .option("header", "true")\
        .option("inferschema", "true")\
        .load(file_path)
    except Exception as e:
        print(f"Error reading file{file_path}: {e}")

    # health_data.printSchema()
    data_validation(health_data)

# to check if all columns are present 
# data validation part is done here
def data_validation(data) :
    mandatory_columns=['patient_id', 'age', 'gender', 'diagnosis_code', 'diagnosis_description', 'diagnosis_date']
    missing_col=[col for col in mandatory_columns if col not in data.columns ]
    if missing_col:
        print(f"{missing_col} is missing from dataframe")
    else :
        print("All mandatory columns are present")
    
# to validate columns are in correct format 
    for data_col,data_type in data.dtypes:
        res=0
        print(data_col,data_type)
        if data_col in schema and data_type==schema[data_col] :
            pass
        elif data_col in schema :
            print(f"incorrect format on {data_col} expected {schema[data_col]} but got {data_type}")
# to handle missing and 



read_file("health_data/health_data_20230801.csv")

