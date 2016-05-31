#!/usr/bin/python
"""
This script was created by Qingyu Feng
on Mar 17, 2016.

This script prepare the precipitation variables to dly files
Later, the dly file will be further processed to apex daily input


"""
# Environmental settings:
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
df_end_time = "2014-12-31 23:59:00"


subdaily_freq = "30Min" # This will be the frequency that 
                            # this proram will  group by from a 
                            # 1min frequency. The parameter should be string.
                            # Examples include: "1H", "2D", "3M" for month.
outf_var_subdaily_zf = r"%s/%s_zf_%s.HLY" \
                    % (infd_lst[0], infd_lst[0],\
                        subdaily_freq)
outf_var_daily_zf = r"%s/%s_zf.DLY" \
                    % (infd_lst[0], infd_lst[0])
outf_var_monthly_zf = r"%s/%s_zf.MLY" \
                    % (infd_lst[0], infd_lst[0])                    
outf_var_annual_zf = r"%s/%s_zf.ALY" \
                    % (infd_lst[0], infd_lst[0])  
                    
outf_var_subdaily_nf = r"%s/%s_nf_%s.HLY" \
                    % (infd_lst[0],infd_lst[0],\
                        subdaily_freq)
outf_var_daily_nf = r"%s/%s_nf.DLY" \
                    % (infd_lst[0], infd_lst[0])
outf_var_monthly_nf = r"%s/%s_nf.MLY" \
                    % (infd_lst[0], infd_lst[0])
outf_var_annual_nf = r"%s/%s_nf.ALY" \
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
        print("Flowdata exists! Good!!!")
    else:
        print("Flowdata not exists! Please check!!!")
    
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

def fill_df_10min_zero(df_10min):
    
    # Create a copy of dataframe for this method
    df_10min_filled_zero = pd.DataFrame.copy(df_10min)

    for dffidx in df_10min_filled_zero.index:
    # The output will be at 5 mins.
    # If there is no var data, the later one will be assigned
        if pd.isnull(df_10min_filled_zero[infd_lst[0]][dffidx]):
            df_10min_filled_zero[infd_lst[0]][dffidx] = 0

    return df_10min_filled_zero



def group_daily_subdaily(df_10min_filled_zero, df_10min):
    
    # We need two output
    # 1. Daily var
    # 2. Subdaily var. The flow data was processed into 30min,
    # here, the var will also be grouped into half hour.
    # var data will all be summed up.
    df_subdaily_var_zf = \
        df_10min_filled_zero.groupby(pd.TimeGrouper(freq=subdaily_freq)).sum()
    df_subdaily_var_zf.insert(1, "Frac_Hour", \
                            df_subdaily_var_zf.index.hour \
                            + df_subdaily_var_zf.index.minute/60.
                            )
    
    df_subdaily_var_nf = \
        df_10min.groupby(pd.TimeGrouper(freq=subdaily_freq)).sum()
    df_subdaily_var_nf.insert(1, "Frac_Hour", \
                            df_subdaily_var_nf.index.hour \
                            + df_subdaily_var_nf.index.minute/60.
                            )
    # Group daily
    df_daily_var_zf = \
        df_10min_filled_zero.groupby(pd.TimeGrouper(freq="1D")).sum()
    df_daily_var_nf = \
        df_10min.groupby(pd.TimeGrouper(freq="1D")).sum()

    # Group monthly mean
    month_sum_avg_zf = \
        df_10min_filled_zero.groupby(pd.TimeGrouper(freq="1M")).sum()
    df_month_var_zf_avg = month_sum_avg_zf.groupby(month_sum_avg_zf.index.month).mean()
    month_sum_avg_nf = \
        df_10min.groupby(pd.TimeGrouper(freq="1M")).sum()
    df_month_var_nf_avg = month_sum_avg_nf.groupby(month_sum_avg_nf.index.month).mean()
    
    # Group monthly standard deviation of daily precipitation std
    month_dailysum_std_zf = \
        df_daily_var_zf.groupby(pd.TimeGrouper(freq="1M")).std()
    df_month_var_zf_std = month_dailysum_std_zf.\
                groupby(month_dailysum_std_zf.index.month).mean()
    
    month_dailysum_std_nf = \
        df_daily_var_nf.groupby(pd.TimeGrouper(freq="1M")).std()
    df_month_var_nf_std = month_dailysum_std_nf.\
                groupby(month_dailysum_std_nf.index.month).mean()    
    

    # Group monthly skew of daily precipitation
    month_sum_skew_zf_mean = \
        df_daily_var_zf.groupby(pd.TimeGrouper(freq="1M")).mean()
    month_sum_skew_zf_median = \
        df_daily_var_zf.groupby(pd.TimeGrouper(freq="1M")).median()
    df_month_var_zf_skew = 3*(month_sum_skew_zf_mean\
                            - month_sum_skew_zf_median)/month_dailysum_std_zf
    df_month_var_zf_skew = df_month_var_zf_skew.\
                            groupby(df_month_var_zf_skew.index.month).mean()
    
    
    month_sum_skew_nf_mean = \
        df_daily_var_nf.groupby(pd.TimeGrouper(freq="1M")).mean()
    month_sum_skew_nf_median = \
        df_daily_var_nf.groupby(pd.TimeGrouper(freq="1M")).median()
    df_month_var_nf_skew = 3*(month_sum_skew_nf_mean\
                            - month_sum_skew_nf_median)/month_dailysum_std_nf
    df_month_var_nf_skew = df_month_var_nf_skew.\
                            groupby(df_month_var_nf_skew.index.month).mean()   
    
    # Group annual
    df_annual_var_zf = \
        df_10min_filled_zero.groupby(df_10min_filled_zero.index.year).sum()
    df_annual_var_nf = \
        df_10min.groupby(df_10min_filled_zero.index.year).sum()
    
    return df_subdaily_var_zf, df_subdaily_var_nf,\
            df_daily_var_zf, df_daily_var_nf,\
            df_month_var_zf_avg, df_month_var_nf_avg,\
            df_month_var_zf_std, df_month_var_nf_std,\
            df_month_var_zf_skew, df_month_var_nf_skew,\
            df_annual_var_zf, df_annual_var_nf
            
#
def accumulate_hlydt(df_subdaily_var_zf, df_subdaily_var_nf):
    # This function is written to generate the accumulated rainfall in one day
    # at different time frequency. Here, it is half hour.
    
    # We need two output
    # 1. Daily var
    # 2. Subdaily var. The flow data was processed into 30min,
    # here, the var will also be grouped into half hour.
    # var data will all be summed up.
#    df_subdaily_cum_var_zf = \
#        df_10min_filled_zero.groupby(\
#            pd.TimeGrouper(\
#                freq=subdaily_freq\
#            )).sum()

    for cumidx in xrange(1,len(df_subdaily_var_zf.index)):

        if (df_subdaily_var_zf.index[cumidx].day == \
            df_subdaily_var_zf.index[cumidx-1].day):
            df_subdaily_var_zf[infd_lst[0]][df_subdaily_var_zf.index[cumidx]] = \
                (df_subdaily_var_zf[infd_lst[0]][df_subdaily_var_zf.index[cumidx]]+
                df_subdaily_var_zf[infd_lst[0]][df_subdaily_var_zf.index[cumidx-1]]\
                )
        else:
            exit
            
        if (df_subdaily_var_nf.index[cumidx].day == \
            df_subdaily_var_nf.index[cumidx-1].day):
            df_subdaily_var_nf[infd_lst[0]][df_subdaily_var_nf.index[cumidx]] = \
                (df_subdaily_var_nf[infd_lst[0]][df_subdaily_var_nf.index[cumidx]]+
                df_subdaily_var_nf[infd_lst[0]][df_subdaily_var_nf.index[cumidx-1]]\
                )
        else:
            exit


def writing_outfiles():
   
    print("Write subdaily var zero filled, hly")
    # write hly
    outfid_var_subdaily_zf = open(outf_var_subdaily_zf, "w")
    
    for dldidx in df_subdaily_var_zf.index:
        outfid_var_subdaily_zf.writelines("%4s%4s%4s%10.2f%10.2f\n" \
            %(pd.to_datetime(dldidx).strftime("%Y"), \
                pd.to_datetime(dldidx).strftime("%m"), \
                pd.to_datetime(dldidx).strftime("%d"), \
                float(df_subdaily_var_zf["Frac_Hour"][dldidx]), \
                float(df_subdaily_var_zf[infd_lst[0]][dldidx]) \
                ))
                
    outfid_var_subdaily_zf.close()
    
    print("Write subdaily var not filled, hly")
    # write hly
    outfid_var_subdaily_nf = open(outf_var_subdaily_nf, "w")
    
    for dldidx in df_subdaily_var_zf.index:
        outfid_var_subdaily_nf.writelines("%4s%4s%4s%10.2f%10.2f\n" \
            %(pd.to_datetime(dldidx).strftime("%Y"), \
                pd.to_datetime(dldidx).strftime("%m"), \
                pd.to_datetime(dldidx).strftime("%d"), \
                float(df_subdaily_var_nf["Frac_Hour"][dldidx]), \
                float(df_subdaily_var_nf[infd_lst[0]][dldidx]) \
                ))              
    outfid_var_subdaily_nf.close()
    


    print("Writing daily precipitation dly zero filled")
    outfid_var_daily_zf = open(outf_var_daily_zf, "w")
    
    for dldidx in df_daily_var_zf.index:
        outfid_var_daily_zf.writelines("%s\t%.2f\n" \
            %(pd.to_datetime(dldidx).strftime("%Y%m%d"), \
                float(df_daily_var_zf[infd_lst[0]][dldidx]) \
                ))                
    outfid_var_daily_zf.close()
    
    print("Writing daily precipitation dly not filled")
    outfid_var_daily_nf = open(outf_var_daily_nf, "w")
    
    for dldidx in df_daily_var_nf.index:
        outfid_var_daily_nf.writelines("%s\t%.2f\n" \
            %(pd.to_datetime(dldidx).strftime("%Y%m%d"), \
                float(df_daily_var_nf[infd_lst[0]][dldidx]) \
                ))
    outfid_var_daily_nf.close()
    
    print("Writing month precipitation dly zero filled")
    outfid_var_monthly_zf = open(outf_var_monthly_zf, "w")
    outfid_var_monthly_zf.writelines("Month\tAveragePrcpmm\tSTDPRCP\tSKEW\n")
    for dldidx in df_month_var_zf_avg.index:
        outfid_var_monthly_zf.writelines("%i\t%.2f\t%.2f\t%.2f\n" \
            %(dldidx, \
            float(df_month_var_zf_avg[infd_lst[0]][dldidx]), \
            float(df_month_var_zf_std[infd_lst[0]][dldidx]), \
            float(df_month_var_zf_skew[infd_lst[0]][dldidx]) \
            ))
    outfid_var_monthly_zf.close()
    
    print("Writing month precipitation dly not filled")
    outfid_var_monthly_nf = open(outf_var_monthly_nf, "w")
    outfid_var_monthly_nf.writelines("Month\tAveragePrcpmm\tSTDPRCP\tSKEW\n")
    for dldidx in df_month_var_nf_avg.index:
        outfid_var_monthly_nf.writelines("%i\t%.2f\t%.2f\t%.2f\n" \
            %(dldidx, \
            float(df_month_var_nf_avg[infd_lst[0]][dldidx]), \
            float(df_month_var_nf_std[infd_lst[0]][dldidx]), \
            float(df_month_var_nf_skew[infd_lst[0]][dldidx]) \
            ))
    outfid_var_monthly_nf.close()    

    
    
    print("Writing annual precipitation dly zero filled")
    outfid_var_annual_zf = open(outf_var_annual_zf, "w")
    
    for dldidx in df_annual_var_zf.index:
        outfid_var_annual_zf.writelines("%i\t%.2f\n" \
            %(dldidx, \
            float(df_annual_var_zf[infd_lst[0]][dldidx])\
            ))
                
    outfid_var_annual_zf.close()
    
    print("Writing annual precipitation dly not filled")
    outfid_var_annual_zf = open(outf_var_annual_zf, "w")
    
    for dldidx in df_annual_var_nf.index:
        outfid_var_annual_zf.writelines("%i\t%.2f\n" \
            %(dldidx, \
            float(df_annual_var_zf[infd_lst[0]][dldidx])\
                ))
                
    outfid_var_annual_zf.close()
    

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

print("Putting precipitation data into timeframe")
df_10min = put_rainfall_to_dataframe(df_10min, lrf_var_all, infd_lst)
print("Finished putting rainfall into timeframe")

print("Filling missing data for flow and conc in dataframe")
df_10min_filled_zero = fill_df_10min_zero(df_10min)
print("Finished filling missing data for flow and conc in dataframe")

print("Aggregating flow and concentration data")
df_subdaily_var_zf, df_subdaily_var_nf,\
df_daily_var_zf, df_daily_var_nf,\
df_month_var_zf_avg, df_month_var_nf_avg,\
df_month_var_zf_std, df_month_var_nf_std,\
df_month_var_zf_skew, df_month_var_nf_skew,\
df_annual_var_zf, df_annual_var_nf\
    = group_daily_subdaily(df_10min_filled_zero, df_10min)
print("Finished aggregating flow and concentration data")

print("Calculating cumulative rainfall in one day for time step")
accumulate_hlydt(df_subdaily_var_zf, df_subdaily_var_nf)
print("Finished calculating cumulative rainfall in one day for time step")

print("Writing output")
writing_outfiles()
print("Finished writing output")


