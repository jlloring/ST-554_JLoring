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
    
    #creates interval validation method
    def interval_chk(self, col: str, lower: str = None, upper: str = None):
        
        #checks that at least one bound is provided
        if lower is None and upper is None:
            print("At least a lower or upper bound must be provied. Please try again.")
            return self #returns itself
        
        #checks that column supplied is in the dataframe
        if col not in self.df.columns:
            print("The column you supplied does not exist in the dataframe. Please try again.")
            return self #returns itself
        
        #grab the dtypes for each column using the dtype attribute and passing to a dictionary
        dtypes_dict = dict(self.df.dtypes)
        col_type = dtypes_dict[col] #stores the type of the column that the user supplies into the method
        
        #check if the user supplied a non-numeric column
        if col_type not in ("float", "int", "longint", "bigint", "double", "integer"):
            print ("The column you supplied is of non-numeric type. Please try again.")
            return self #returns itself
        
        #return True/False based on whether or not column value is in range supplied, account for NULL
        
        
        
        
        
