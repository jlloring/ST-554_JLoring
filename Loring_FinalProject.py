# Name: Jamie Loring
# Due Date: 4/30/2026
# Purpose: ST 554 Final Project, Produce Data Section


#import the required modules
import pandas as pd
import time

#read in power_streaming_data_csv as a regular pandas dataframe
pwr_strm_dat = pd.read_csv("power_streaming_data.csv")

#initialize for loop
for i in range(20):
    
    #randomly sample 5 rows
    sampled_5 = pwr_strm_dat.sample(n = 5)
    
    #use f string to store each sampled file as its own.csv in the csv_files_final_folder
    sampled_file_name = f"csv_files_final/samples_{i}.csv"
    
    #output each sampled file to a csv in the csv_files_final folder
    sampled_5.to_csv(sampled_file_name, index = False) #prevents indices from being written out
    
    #pauses for 30 seconds in between outputting of data sets
    time.sleep(30)
    
    
#Note: This entire .py file can now be submitted in a python console!