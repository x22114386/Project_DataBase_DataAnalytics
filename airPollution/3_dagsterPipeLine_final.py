import sys
import traceback
from dagster import Out, Output, job, op
from dbcon import mySqlEngine, postgresEngine
import pandas as pd
from datetime import datetime


@op(out={"df": Out(is_required=True)})
def extractDataFromMysql(context):
    try:
        with mySqlEngine().connect() as conn:
            #fetch data from mysql db to data frame
            context.log.info(conn)
            df = pd.read_sql("select * from PollutionData", conn)
            # context.log.info("df head is ->" + df.head())
            yield Output(df, "df")
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)

@op(out={"df": Out(is_required=True)})
def removeColumnPollutantUnit(context, df):
    try:
        # remove rows with NA values in column C
        df = df.dropna(subset=['pollutant_unit'])
        print("The column is deleted")
        yield Output(df, "df")
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)

@op(out={"df": Out(is_required=True)})
def replaceNullValuesPollutantMin(context, df):
    try:
        print("before missing value in pollutant_min")
        print(df)
        # remove rows with NA values in column C
        df['pollutant_min'] = df['pollutant_min'].fillna(0)
        print("after deletion")
        print(df)
        yield Output(df, "df")
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)

@op(out={"df": Out(is_required=True)})
def replaceNullValuesPollutantMax(context, df):
    try:
        print("before missing value in pollutant_max")
        print(df)
        # remove rows with NA values in column C
        df['pollutant_max'] = df['pollutant_max'].fillna(0)
        print("after deletion")
        print(df)
        yield Output(df, "df")
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)

@op(out={"df": Out(is_required=True)})
def replaceNullValuesPollutantAvg(context, df):
    try:
        print("before missing value in pollutant_avg")
        print(df)
        # remove rows with NA values in column C
        df['pollutant_avg'] = df['pollutant_avg'].fillna(0)
        print("after deletion")
        print(df)
        yield Output(df, "df")
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)

@op(out={"df": Out(is_required=True)})
def addingNewColumnCurrentDateTime(context, df):
    try:
# define a function to return the current date and time
        def get_current_datetime():
            return datetime.now()

# add a new column with the current date and time using the apply() method
        df['Date_Time'] = df.apply(lambda row: get_current_datetime(), axis=1)

# print the resulting DataFrame
        print(df)
        yield Output(df, "df")
    
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)    
        

@op
def insertDataToPostgres(context, df):
    try:
        noOfRowsInserted = 0
        context.log.info(f'importing rows {noOfRowsInserted} to {noOfRowsInserted + len(df)}')
        engine = postgresEngine()
        database = 'custom'
        table = "PollutionData"
            
        #insert data to postgres table
        df.to_sql(table, con=engine, schema=database, if_exists='replace')
        noOfRowsInserted += len(df)
        context.log.info("Data imported successful")

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(f"Caught exception {exc_type} with message {e}")
        print("Traceback:")
        traceback.print_tb(exc_traceback)


@job
def runJob():
    df = extractDataFromMysql()
    df = removeColumnPollutantUnit(df)
    df = replaceNullValuesPollutantMin(df)
    df = replaceNullValuesPollutantMax(df)
    df = replaceNullValuesPollutantAvg(df)
    df = addingNewColumnCurrentDateTime(df)
    insertDataToPostgres(df)
    


    
if __name__ == "__main__":
    result = runJob.execute_in_process()
    print(result)