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
    def interval_chk(self, col_name: str, lower = None, upper = None):
        
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
            cond = F.col(col_name).between(lower, upper) #condition when both bounds are supplied
        elif lower is not None:
            cond = F.col(col_name) >= lower #condition when only lower bound is supplied
        else:
            cond = F.col(col_name) <= upper #condition when only upper bound is supplied
            
        #create Interval_Check column and account for NULL values
        self.df = self.df.withColumn("Interval_Check", F.when(F.col(col_name).isNull(), None).otherwise(cond))
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
        if col_type != "string":
            print ("The column you supplied is of non-string type. Please try again.")
            return self #returns itself
            
        #create Levels_Check column and account for NULL values
        self.df = self.df.withColumn("Levels_Check", F.when(F.col(col_name).isNull(), None) \
                                     .otherwise(F.col(col_name).isin(levels)))
        return self #returns itself
    
    #creates missing validation method
    def missing_chk(self, col_name: str):
        
        #checks that column supplied is in the dataframe
        if col_name not in self.df.columns:
            print("The column you supplied does not exist in the dataframe. Please try again.")
            return self #returns itself
        
        #create Missing_Check column
        self.df = self.df.withColumn("Missing_Check", F.col(col_name).isNull())
        return self #returns itself
    
    ## Summarization Methods
    
    #creates min & max method
    def min_max(self, col_name: str = None, group_var: str = None):
    
        #grab the dtypes for each column using the dtype attribute and passing to a dictionary
        dtypes_dict = dict(self.df.dtypes)
        
        #executing method when col_name is not None
        if col_name is not None:
            
            #checks that column supplied is in the dataframe
            if col_name not in self.df.columns:
                print("The column you supplied does not exist in the dataframe. Please try again.")
                return None #returns None per instructions
            
            #check if the user supplied a non-numeric column
            col_type = dtypes_dict[col_name] #stores the type of the column that the user supplies into the method
            if col_type not in ("float", "int", "longint", "bigint", "double", "integer"):
                print ("The column you supplied is of non-numeric type. Please try again.")
                return None #returns None per instructions
            
            if group_var is not None:
                
                #checks that group_var is in the dataframe
                if group_var not in self.df.columns:
                    print("The grouping variable you supplied does not exist in the dataframe. Please try again.")
                    return None #returns None per instructions
                else:
                    return self.df.groupBy(group_var).agg(F.min(col_name), F.max(col_name)).toPandas()
            
            else:
                return self.df.agg(F.min(col_name), F.max(col_name)).toPandas()
            
        #executing method when col_name is None
        else:
            #store all numeric columns
            numeric_columns = []
            
            #collect the dtype of all columns
            for i in self.df.columns:
                dtype = dtypes_dict[i]
                
                #only append numeric columns to list
                if dtype in ("float", "int", "longint", "bigint", "double", "integer"):
                    numeric_columns.append(i)
                
            #store mins and maxes in separate lists, bring them together, then unpack them
            min_list = []
            max_list = []
            
            #append mins and maxes to their separate lists
            for x in numeric_columns:
                min_list.append(F.min(x))
                max_list.append(F.max(x))
               
            #combine min and max lists
            full_list = min_list + max_list
            
            if group_var is not None:
                
                #checks that group_var is in the dataframe
                if group_var not in self.df.columns:
                    print("The grouping variable you supplied does not exist in the dataframe. Please try again.")
                    return None #returns None per instructions
                else:
                    return self.df.groupBy(group_var).agg(*full_list).toPandas()
            
            else:
                return self.df.agg(*full_list).toPandas()
        
    #creates string counts method
    def string_counts(self, col_name1: str, col_name2: str = None):
        
        #grab the dtypes for each column using the dtype attribute and passing to a dictionary
        dtypes_dict = dict(self.df.dtypes)
        
        #if only 1 column is supplied:
        if col_name2 is None:
            
            #checks that column 1 supplied is in the dataframe
            if col_name1 not in self.df.columns:
                print("The first column you supplied is not in the dataframe. Please try again.")
                return None #returns None
            
            #checks that column1 is a string
            if dtypes_dict[col_name1] != "string":
                print ("The first column you supplied is of non-string type. Please try again.")
                return None #returns None
            
            return self.df.groupBy(col_name1).count().toPandas()
        
        #if both columns are supplied
        else:
            
            #checks that column 1 supplied is in the dataframe
            if col_name1 not in self.df.columns:
                print("The first column you supplied is not in the dataframe. Please try again.")
                return None #returns None
            
            #checks that column1 is a string
            if dtypes_dict[col_name1] != "string":
                print ("The first column you supplied is of non-string type. Please try again.")
                return None #returns None
            
            #checks that column 2 supplied is in the dataframe
            if col_name2 not in self.df.columns:
                print("The second column you supplied is not in the dataframe. Please try again.")
                return None #returns None
            
            #checks that column2 is a string
            if dtypes_dict[col_name2] != "string":
                print ("The second column you supplied is of non-string type. Please try again.")
                return None #returns None
            
            return self.df.groupBy(col_name1, col_name2).count().toPandas()