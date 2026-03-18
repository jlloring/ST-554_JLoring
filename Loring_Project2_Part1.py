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