#!/usr/bin/python
"""
This script was created by Qingyu Feng
on Mar 17, 2016.

"""
# Environmental settings:
# 1. For generating time object
import datetime
# 2. for test file existance
import os
# 3. For resample the data points
import pandas as pd
import numpy as np

# Input files and folders:
# +++++++++++++++++++++++++++++++++++++++++++++++++++++
# Input: mgt updated templates
# This need to be prepared manually
# Input files and folders:
# +++++++++++++++++++++++++++++++++++++++++++++++++++++
# Missing data from AD, AS1, AS2, ALG, ADWT will be readin
# for filling of AS1 and AS2, which are the target simulation
# field sites.
# For other sites, they will be processed at daily step.
#infd_lst = ["AD", "AS1", "AS2", "ALG", "ADWT"]
#print(infd_lst)
# The number of sites used to fill the missing data.
infd_lst = ["AD", "ADWT", "AS1", "AS2", "AXL"]

inf_lst = [0]*len(infd_lst)
for fdidx in xrange(len(infd_lst)):
    inf_lst[fdidx] = r"%s/%s.orig" %(infd_lst[fdidx], infd_lst[fdidx])
print(inf_lst)

df_start_time = "2007-01-01 00:00:00"
df_end_time = "2007-12-31 23:59:00"

outf_var_daily_ntf = r"%s/%s_ntf.DLY" \
                    % (infd_lst[0], infd_lst[0])
outf_var_month_ntf = r"%s/%s_ntf.MLY" \
                    % (infd_lst[0], infd_lst[0])                    
                    
outf_var_daily_nf = r"%s/%s_nf.DLY" \
                    % (infd_lst[0], infd_lst[0])
outf_var_month_nf = r"%s/%s_nf.MLY" \
                    % (infd_lst[0], infd_lst[0])

# Functions:
###############################################################################
def generate_timeframe(df_start_time, df_end_time, infd_lst):
    
    df_10min_index = pd.date_range(df_start_time,\
                                    df_end_time,\
                                    freq = "10Min")

    # Two columns need to be included:
    # 1. The precipitation data.
    # 2. The time interval. For subdaily simulation, the time fraction
    # need to be calculated as fraction of hours. 
    # This fraction of hours will be calculated after grouping.
    df_10min_columns = list(infd_lst)
    df_10min =  pd.DataFrame(np.zeros((len(df_10min_index), \
                                len(df_10min_columns))), \
                                columns = df_10min_columns, \
                                index = df_10min_index,\
                                dtype=float)
    for dfcidx in df_10min_columns:
        df_10min[dfcidx] = np.nan

    return df_10min




def read_var(siteidx):

    if os.path.isfile(inf_lst[siteidx]):
        print("Windspeed data exists! Good!!!")
    else:
        print("Windspeed data not exists! Please check!!!")
    
    lrf_var = 0
    
    rf_var = open(inf_lst[siteidx], "r")
    lrf_var = rf_var.readlines()
    rf_var.close()
    
    del lrf_var[0]
    
    for lrfidx in range(len(lrf_var)):

        lrf_var[lrfidx] = lrf_var[lrfidx].split("\t")
        lrf_var[lrfidx][0] = pd.to_datetime(lrf_var[lrfidx][1] +
                                " " + lrf_var[lrfidx][2])
        lrf_var[lrfidx][-1] = lrf_var[lrfidx][-1][:-1]
#        print(lrf_var[lrfidx])

        # In case there is a missing value on some time points
        if lrf_var[lrfidx][-1] == '':
            lrf_var[lrfidx][-1] = np.nan
#        print(lrf_var[lrfidx])
        if not lrf_var[lrfidx][-1] == np.nan:
            lrf_var[lrfidx][-1] = float(lrf_var[lrfidx][-1])


        lrf_var[lrfidx] = (lrf_var[lrfidx][0],lrf_var[lrfidx][-1])
#        print(lrf_var[lrfidx])
    lrf_var = dict(lrf_var)
        # Pay attention to the last column of your data. There should
        # be an empty line. The reason is to have a \n symble. If not,
        # The values will be cut off by this operation.
    return lrf_var

def put_rainfall_to_dataframe(df_10min, lrf_var_all, infd_lst):
     
    for dfidx in df_10min.index:
        # Put first site in site list
        for slidx in xrange(len(infd_lst)):
            if pd.Timestamp(dfidx) in lrf_var_all[slidx]:
                df_10min[infd_lst[slidx]][dfidx] = \
                    lrf_var_all[slidx][pd.Timestamp(dfidx)]
                                            
        # Fill AS1 if there is missing data from AS2.
        # With more sites, AD, ALG, the missing data number are similar.
        
        if pd.isnull(df_10min[infd_lst[0]][dfidx]):
            if pd.notnull(df_10min[infd_lst[1]][dfidx]):
                df_10min[infd_lst[0]][dfidx] = float(df_10min[infd_lst[1]][dfidx])
            elif pd.notnull(df_10min[infd_lst[2]][dfidx]):
                df_10min[infd_lst[0]][dfidx] = float(df_10min[infd_lst[2]][dfidx])
            elif pd.notnull(df_10min[infd_lst[3]][dfidx]):
                df_10min[infd_lst[0]][dfidx] = float(df_10min[infd_lst[3]][dfidx])                
            elif pd.notnull(df_10min[infd_lst[4]][dfidx]):
                df_10min[infd_lst[0]][dfidx] = float(df_10min[infd_lst[4]][dfidx])
            else:
                df_10min[infd_lst[0]][dfidx] = df_10min[infd_lst[0]][dfidx]
               
    return df_10min

def fill_df_10min_nt(df_10min):
    
    # Create a copy of dataframe for this method
    df_10min_filled_nt = pd.DataFrame.copy(df_10min)

    for dffidx in range(len(df_10min_filled_nt.index)-1):
    # The output will be at 5 mins.
    # If there is no var data, the later one will be assigned
        if pd.isnull(df_10min_filled_nt[infd_lst[0]][df_10min_filled_nt.index[dffidx]]):
            df_10min_filled_nt[infd_lst[0]][df_10min_filled_nt.index[dffidx]]\
		= df_10min_filled_nt[infd_lst[0]][df_10min_filled_nt.index[dffidx+1]]

    return df_10min_filled_nt



def group_daily_subdaily(df_10min_filled_nt, df_10min):
    
    # We need two output
    # 1. Daily var
    # 2. Subdaily var. The flow data was processed into 30min,
    # here, the var will also be grouped into half hour.
    # var data will all be summed up.

    # Group to daily mean
    df_daily_var_nf = \
        df_10min.groupby(pd.TimeGrouper(freq="1D")).mean()
        
    df_daily_var_ntf = \
        df_10min_filled_nt.groupby(pd.TimeGrouper(freq="1D")).mean()
    

    # Group monthly mean
    month_mean_ntf = df_10min_filled_nt.\
                groupby(pd.TimeGrouper(freq="1M")).mean()
    df_month_var_avg_ntf = month_mean_ntf.\
                groupby(month_mean_ntf.index.month).mean()
                
    month_mean_nf = df_10min.\
                groupby(pd.TimeGrouper(freq="1M")).mean()
    df_month_var_avg_nf = month_mean_nf.\
                groupby(month_mean_nf.index.month).mean()
    
    # Group monthly std
    month_mean_std_ntf = df_10min_filled_nt.\
                groupby(pd.TimeGrouper(freq="1M")).mean()
    df_month_var_avg_std_ntf = month_mean_std_ntf.\
                groupby(month_mean_std_ntf.index.month).std()
                
    month_mean_std_nf = df_10min.\
                groupby(pd.TimeGrouper(freq="1M")).mean()
    df_month_var_avg_std_nf = month_mean_std_nf.\
                groupby(month_mean_std_nf.index.month).std()
    
    return df_daily_var_nf, df_daily_var_ntf,\
	    df_month_var_avg_ntf,df_month_var_avg_nf,\
	    df_month_var_avg_std_ntf, df_month_var_avg_std_nf

def writing_outfiles():
   
    print("Writing daily average wind speed with next day filled")
    outfid_var_daily_ntf = open(outf_var_daily_ntf, "w") 
    for dldidx in df_daily_var_ntf.index:
        outfid_var_daily_ntf.writelines("%s\t%.2f\n" \
            %(dldidx, \
                float(df_daily_var_ntf[infd_lst[0]][dldidx])\
                ))                
    outfid_var_daily_ntf.close()
 
    print("Writing daily average wind speed with no filled")
    outfid_var_daily_nf = open(outf_var_daily_nf, "w")
    for dldidx in df_daily_var_nf.index:
        outfid_var_daily_nf.writelines("%s\t%.2f\n" \
            %(dldidx, \
                float(df_daily_var_nf[infd_lst[0]][dldidx])\
                ))
    outfid_var_daily_nf.close()

    print("Writing monthly mean and std of relative humidity with no filled")
    outfid_var_month_nf = open(outf_var_month_nf, "w")
    outfid_var_month_nf.writelines("Month\tAvg\tSTD\n")
    for dldidx in df_month_var_avg_nf.index:
        outfid_var_month_nf.writelines("%i\t%.2f\t%.2f\n" \
            %(dldidx,\
                float(df_month_var_avg_nf[infd_lst[0]][dldidx]),\
                float(df_month_var_avg_std_nf[infd_lst[0]][dldidx])\
                ))
    outfid_var_month_nf.close()

    print("Writing monthly mean and std of relative humidity with next time filled")
    outfid_var_month_ntf = open(outf_var_month_ntf, "w")
    outfid_var_month_ntf.writelines("Month\tAvg\tSTD\n")
    for dldidx in df_month_var_avg_ntf.index:
        outfid_var_month_ntf.writelines("%i\t%.2f\t%.2f\n" \
            %(dldidx,\
                float(df_month_var_avg_ntf[infd_lst[0]][dldidx]),\
                float(df_month_var_avg_std_ntf[infd_lst[0]][dldidx])\
                ))
    outfid_var_month_ntf.close() 
 
# Calling Functions:
print("Generating time frames")
df_10min = generate_timeframe(df_start_time, df_end_time, infd_lst)
print("Finished generating timeframe")

lrf_var_all = [0]*len(inf_lst)
start_time_all = [0]*len(inf_lst)
end_time_all = [0]*len(inf_lst)

# Readin all sites
for siteidx in xrange(len(inf_lst)):
    lrf_var = read_var(siteidx)
    lrf_var_all[siteidx] = lrf_var    

print("Putting relative humidity data into timeframe")
df_10min = put_rainfall_to_dataframe(df_10min, lrf_var_all, infd_lst)
print("Finished putting relative humidity into timeframe")

print("Filling missing data for relative humidity in dataframe")
df_10min_filled_nt = fill_df_10min_nt(df_10min)
print("Finished filling missing data for relative humidity in dataframe")
#
print("Aggregating relative humidity data")
df_daily_var_nf, df_daily_var_ntf,\
df_month_var_avg_ntf,df_month_var_avg_nf,\
df_month_var_avg_std_ntf, df_month_var_avg_std_nf\
	= group_daily_subdaily(df_10min_filled_nt, df_10min)
print("Finished aggregating relative humidity data")

print("Writing output")
writing_outfiles()
print("Finished writing output")


                    
                    
                    
                    
                    
                    
                    
                    
