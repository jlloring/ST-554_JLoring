## Part 1: Creating a Class


#import the required modules
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce
from pyspark.sql.types import *
import pandas as pd


# define the given class name
class SparkDataCheck:
    
    #creates initial attributes per instructions
    def __init__(self, dataframe): 
        self.df = dataframe
        
    #creates first @classmethod - creates an instance while reading in a csv file
    @classmethod
    def reading_csv(cls, spark, file_path):
        """
        Create an instance of the SparkDataCheck class while reading in a csv file
        
        Arguments:
        cls -- reference to the class
        spark -- the spark session
        file_path -- the path to the csv file
        """
        df = spark.read.load(file_path, format="csv", sep=",", inferSchema="true", header="true")
        return cls(df)
    
    #creates second @classmethod - creates an instance from a (standard) pandas dataframe
    @classmethod
    def pandas_dataframe(cls, spark, pd_df):
        """
        Create an instance of the SparkDataCheck class from a pandas dataframe
        
        Arguments:
        cls -- reference to the class
        spark -- the spark session
        pd_df -- the pandas dataframe
        """
        df = spark.createDataFrame(pd_df)
        return cls(df)
    
    ## Validation Methods
    
    #creates interval validation method
    def interval_chk(self, col_name: str, lower: str = None, upper: str = None):
        
        #checks that at least one bound is provided
        if lower is None and upper is None:
            print("At least a lower or upper bound must be provied. Please try again.")
            return self #returns itself
        
        #checks that column supplied is in the dataframe
        if col_name not in self.df.columns:
            print("The column you supplied does not exist in the dataframe. Please try again.")
            return self #returns itself
        
        #grab the dtypes for each column using the dtype attribute and passing to a dictionary
        dtypes_dict = dict(self.df.dtypes)
        col_type = dtypes_dict[col_name] #stores the type of the column that the user supplies into the method
        
        #check if the user supplied a non-numeric column
        if col_type not in ("float", "int", "longint", "bigint", "double", "integer"):
            print ("The column you supplied is of non-numeric type. Please try again.")
            return self #returns itself
        
        #return True/False based on whether or not column value is in range supplied, account for NULL
        #build condition checker for supplying into withColumn
        if lower is not None and upper is not None:
            cond = col(col_name).between(lower, upper) #condition when both bounds are supplied
        elif lower is not None:
            cond = col(col_name) >= lower #condition when only lower bound is supplied
        else:
            cond = col(col_name) <= upper #condition when only upper bound is supplied
            
        #create Interval_Check column and account for NULL values
        self.df = self.df.withColumn("Interval_Check", when(col(col_name).isNull(), None).otherwise(cond))
        return self #returns itself
    
    #creates levels validation method
    def levels_chk(self, col_name: str, levels):
        
        #checks that column supplied is in the dataframe
        if col_name not in self.df.columns:
            print("The column you supplied does not exist in the dataframe. Please try again.")
            return self #returns itself
        
        #grab the dtypes for each column using the dtype attribute and passing to a dictionary
        dtypes_dict = dict(self.df.dtypes)
        col_type = dtypes_dict[col_name] #stores the type of the column that the user supplies into the method
        
        #check if the user supplied a non-string column
        if col_type not in ("string"):
            print ("The column you supplied is of non-string type. Please try again.")
            return self #returns itself
            
        #create Levels_Check column and account for NULL values
        self.df = self.df.withColumn("Levels_Check", when(col(col_name).isNull(), None).otherwise(col(col_name).isin(levels)))
        return self #returns itself
    
    # creates missing validation method
    def missing_chk(self, col_name: str):
        
        #checks that column supplied is in the dataframe
        if col_name not in self.df.columns:
            print("The column you supplied does not exist in the dataframe. Please try again.")
            return self #returns itself
        
        #create Missing_Check column
        self.df = self.df.withColumn("Missing_Check", when(col(col_name).isNull(), True).otherwise(False))
        return self #returns itself
    
    ## Summarization Methods
    
    