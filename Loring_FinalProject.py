# Name: Jamie Loring
# Due Date: 4/30/2026
# Purpose: ST 554 Final Project, Produce Data Section


#import the required modules
import pandas as pd

#read in power_streaming_data_csv as a regular pandas dataframe
pwr_strm_dat = pd.read_csv("power_streaming_data.csv")

#initialize for loop
for i in range(20):
    
    #randomly sample 5 rows
    sampled_5 = pwr_strm_dat.sample(n = 5)
    
    #output above to .csv file in csv_files_final folder
    sampled_5.to_csv("samples.csv", index = False) #prevents indices from being written out
    
    ##STILL NEED TO Pause for 10 seconds in between outputting of data sets