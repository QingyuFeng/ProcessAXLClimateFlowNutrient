"""
This script was created by Qingyu Feng
on Mar 17, 2016.


"""

# Environmental settings:
# 2. for test file existance
import os
# 3. For resample the data points
import pandas as pd
import numpy as np

# Input files and folders:
###############################################################################
# Input: mgt updated templates
# This need to be prepared manually
site_name = "AS1" 

infd_flow = "dl06flow"
infd_conc = "dl07conc"
if not os.path.isdir(infd_flow): print("Observed data for flow does not exist")
if not os.path.isdir(infd_conc): print("Observed data for conc does not exist")



inf_flow = r"%s/%s/%s.orig" %(infd_flow, site_name, site_name)
inf_conc = r"%s/%s/%s.orig" %(infd_conc, site_name, site_name)

subdaily_freq = "30Min" # This will be the frequency that 
                            # this proram will  group by from a 
                            # 1min frequency. The parameter should be string.
                            # Examples include: "1H", "2D", "3M" for month.

outfd_load_daily = "dl10_load_insjsites"
if not os.path.isdir(outfd_load_daily): os.mkdir(outfd_load_daily)

outfd_load_site = r"%s/%s" %(outfd_load_daily, site_name)
if not os.path.isdir(outfd_load_site):
    os.mkdir(outfd_load_site)
    print("Output folder for ", site_name, "was created")


# Functions:
###############################################################################
def read_flow(inf_flow):
    
    if os.path.isfile(inf_flow):
        print("Flowdata exists! Good!!!")
    else:
        print("Flowdata not exists! Please check!!!")
    
    rf_flow = open(inf_flow, "r")
    lrf_flow = rf_flow.readlines()
    rf_flow.close()

    del lrf_flow[0]    
    
    # Get start time and end time for generating timeframes    
    for lrfidx in range(len(lrf_flow)):

        lrf_flow[lrfidx] = lrf_flow[lrfidx].split("\t")
        #print(lrf_flow[lrfidx][1] + " " + lrf_flow[lrfidx][2])
        lrf_flow[lrfidx][0] = pd.to_datetime(lrf_flow[lrfidx][1] +
                                " " + lrf_flow[lrfidx][2])
        lrf_flow[lrfidx][-1] = lrf_flow[lrfidx][-1][:-1]
        #print(lrf_flow[lrfidx])

        # There are some days that values are missing, but they have days.
        # This will affect the following calculation.
        # These need to be deleted.
        if lrf_flow[lrfidx][-1] == '':
            lrf_flow[lrfidx][-1] = np.nan
        
#        print(lrf_flow[lrfidx])

        lrf_flow[lrfidx] = (lrf_flow[lrfidx][0],lrf_flow[lrfidx][-1])        
        
    lrf_flow = dict(lrf_flow)
        # Pay attention to the last column of your data. There should
        # be an empty line. The reason is to have a \n symble. If not,
        # The values will be cut off by this operation.
        
    return lrf_flow


def read_conc(inf_conc):

    if os.path.isfile(inf_conc):
        print("ConcData exists! Good!!!")
    else:
        print("ConcData not exists! Please check!!!")
    
    rf_conc = open(inf_conc, "r")
    lrf_conc = rf_conc.readlines()
    rf_conc.close()
    
    del lrf_conc[0]    

    
    # Processing each line to get the values
    for lrfidx in range(len(lrf_conc)):
#        print("before", lrf_conc[lrfidx])

        lrf_conc[lrfidx] = lrf_conc[lrfidx].split("\t")
#        print(lrf_conc[lrfidx])
        lrf_conc[lrfidx][0] = pd.to_datetime(lrf_conc[lrfidx][1] +
                                " " + lrf_conc[lrfidx][2])
#        if not lrf_conc[lrfidx][0].minute%2 == 0:
#            lrf_conc[lrfidx][0] = lrf_conc[lrfidx][0] + pd.Timedelta("1min")
#        print(lrf_conc[lrfidx][0])
        
        lrf_conc[lrfidx][-1] = lrf_conc[lrfidx][-1][:-1] 
#        print("Processing missing data for conc. ")
    # In loadest, missing data is marked as "-9999.0"
        
        for ilidx in range(len(lrf_conc[lrfidx])):
            if lrf_conc[lrfidx][ilidx] == "":
                lrf_conc[lrfidx][ilidx] = np.nan
        lrf_conc[lrfidx] = (lrf_conc[lrfidx][0], lrf_conc[lrfidx][3:])
    lrf_conc = dict(lrf_conc)
#    print("after",lrf_conc)
    return lrf_conc 

# For the calculation of event load, I will use a new dataframe.
# This data structure will do the calculation for each day.
# The old data structure will be used only for generating subdaily
# flow data.

def generate_timeframe(df_start_time, df_end_time):
    
    # Initiate variable values
    df_1min = 0    
    df_1min_index = 0
    
  
    
    df_1min_index = pd.date_range(df_start_time, df_end_time, freq = "1min")
                        
    df_1min = pd.DataFrame(np.zeros((len(df_1min_index),\
                                len(df_1min_columns))),\
                                columns = df_1min_columns,\
                                index = df_1min_index)   
    
    for dfcidx in df_1min_columns:
        df_1min[dfcidx] = np.nan
    
    return df_1min

# After generating the list of each day, I will have to put the value from
# observed list to the frame
def put_flow_conc_todataframe(df_1min, lrf_flow, lrf_conc):
    
    for dfidx in range(len(df_1min.index)):
        
        if pd.Timestamp(df_1min.index[dfidx]) in lrf_flow:
            df_1min["Flow(l/s)"][dfidx] = lrf_flow[pd.Timestamp\
                                            (df_1min.index[dfidx])]
        if pd.Timestamp(df_1min.index[dfidx]) in lrf_conc:
#            print(lrf_conc[pd.Timestamp(df_1min.index[dfidx]]))
            df_1min["Ammonia(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
                                            (df_1min.index[dfidx])][0])
            df_1min["NO3NO2(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
                                            (df_1min.index[dfidx])][1])                                           
            df_1min["TotalN(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
                                            (df_1min.index[dfidx])][2])
            df_1min["TotalP(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
                                            (df_1min.index[dfidx])][3])
            df_1min["OrthoP(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
                                            (df_1min.index[dfidx])][4])
            df_1min["Sediment(g/L)"][dfidx] = float(lrf_conc[pd.Timestamp\
                                            (df_1min.index[dfidx])][5])
            
              
                
    return df_1min



def fill_df_1min_nextmin(df_1min):
    
    # Create a list that stores value for load availability.
    # This will be a two dimentional array containing time
    # and flow.
    # Data structure: numpy array    
    amo_load_list = []
    no23_load_list = []
    tn_load_list = []
    tp_load_list = []
    op_load_list = []
    sedi_load_list = []
    
    for dffidx in xrange(len(df_1min.index)-2):
        # The output will be at 1 mins.
        # If there is no flow data, the value for the next time will be assigned
        # The original unit of flow is liter per second
        # In loadest, the flow unit could be cubic feet per second.
        # The unit will be converted when the data are write into output
        
        if pd.isnull(df_1min["Flow(l/s)"][dffidx]):
            df_1min["Flow(l/s)"][dffidx] =\
                df_1min["Flow(l/s)"][dffidx+1]
        if pd.isnull(df_1min["Flow(l/s)"][dffidx]):
            df_1min["Flow(l/s)"][dffidx] =\
                df_1min["Flow(l/s)"][dffidx+2]
        if pd.isnull(df_1min["Flow(l/s)"][dffidx]):
            df_1min["Flow(l/s)"][dffidx] =\
                df_1min["Flow(l/s)"][dffidx+3]
                
            # Calculating load for when conc is available
        df_1min["AmmoniaLoad(mg/s)"][dffidx]=\
            df_1min["Ammonia(ppm)"][dffidx]*\
                df_1min["Flow(l/s)"][dffidx]
        # There will be 5 variables in one element, 
        # Time, Flow, Flow for accumulation, load, linear filled load
        if (df_1min["Flow(l/s)"][dffidx] > 0.01 or\
            pd.notnull(df_1min["AmmoniaLoad(mg/s)"][dffidx])):
            amo_load_list.append([df_1min.index[dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["AmmoniaLoad(mg/s)"][dffidx], 0.00])
        df_1min["NO3NO2Load(mg/s)"][dffidx]=\
            df_1min["NO3NO2(ppm)"][dffidx]*\
                df_1min["Flow(l/s)"][dffidx]
        if (df_1min["Flow(l/s)"][dffidx] > 0.01 or\
            pd.notnull(df_1min["AmmoniaLoad(mg/s)"][dffidx])):
            no23_load_list.append([df_1min.index[dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["AmmoniaLoad(mg/s)"][dffidx], 0.00])
                
        df_1min["TotalNLoad(mg/s)"][dffidx]=\
            df_1min["TotalN(ppm)"][dffidx]*\
                df_1min["Flow(l/s)"][dffidx] 
        if (df_1min["Flow(l/s)"][dffidx] > 0.01 or\
            pd.notnull(df_1min["TotalNLoad(mg/s)"][dffidx])):
            tn_load_list.append([df_1min.index[dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["TotalNLoad(mg/s)"][dffidx], 0.00])                
            
        df_1min["TotalPLoad(mg/s)"][dffidx]=\
            df_1min["TotalP(ppm)"][dffidx]*\
                df_1min["Flow(l/s)"][dffidx]
        if (df_1min["Flow(l/s)"][dffidx] > 0.01 or\
            pd.notnull(df_1min["TotalPLoad(mg/s)"][dffidx])):
            tp_load_list.append([df_1min.index[dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["TotalPLoad(mg/s)"][dffidx], 0.00])
                
        df_1min["OrthoPLoad(mg/s)"][dffidx]=\
            df_1min["OrthoP(ppm)"][dffidx]*\
                df_1min["Flow(l/s)"][dffidx] 
        if (df_1min["Flow(l/s)"][dffidx] > 0.01 or\
            pd.notnull(df_1min["OrthoPLoad(mg/s)"][dffidx])):
            op_load_list.append([df_1min.index[dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["OrthoPLoad(mg/s)"][dffidx], 0.00])                
                
        df_1min["SedimentLoad(g/s)"][dffidx]=\
            df_1min["Sediment(g/L)"][dffidx]*\
                df_1min["Flow(l/s)"][dffidx]  
        if (df_1min["Flow(l/s)"][dffidx] > 0.01 or\
            pd.notnull(df_1min["SedimentLoad(g/s)"][dffidx])):
            sedi_load_list.append([df_1min.index[dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["Flow(l/s)"][dffidx],\
                df_1min["SedimentLoad(g/s)"][dffidx], 0.00])

    return df_1min, amo_load_list, no23_load_list\
    , tn_load_list, tp_load_list, op_load_list\
    , sedi_load_list


# After getting the concentration, I need to process them.
# The goal was to do the linear regression.
# The core part was to identify the starting and ending time of the event.
# To identify the starting, here I assumed it to be the first value of 0.02.
# To identify the first value, one trick is to use the cumulative value.
# To identify the last value, the cumulative minus the former time point
# would be the point. But this could be the starting of another event. 
# The longest event would be 3 days. So, if the time difference between
# two points is larger than 3 day, it will be identified as the end of event.
def linear_regression(loadlist):
    
    load_lr = list(loadlist)    
    stopx = 1
    stopy = 1
    xload = 0
    yload = 0
    
    for allidx in xrange(1, len(load_lr)):
        # first update the flow data to be the accumulative flow for each 
        # event. This calculation would be terminated by defining
        # time interval less than 3 days
        if (load_lr[allidx][0]-load_lr[allidx-1][0]).days < 3:
            load_lr[allidx][2] = load_lr[allidx][2]+\
                                        load_lr[allidx-1][2]
        if load_lr[allidx-1][2] == 0.02:
            load_lr[allidx-1][3] = 0.00
        load_lr[allidx] = load_lr[allidx]
#        print((amo_load_list[allidx][0] - amo_load_list[allidx-1][0]).days)


#        print(type(load_lr[allidx][3]))
        if not np.isnan(load_lr[allidx][3]):

            xload = yload
            yload = load_lr[allidx][3]
            stopx = stopy
            stopy = allidx - 1

            cof_a = (xload - yload)/(stopx - stopy)
            cof_b = (stopx*yload - stopy*xload)/(stopx - stopy)
            for stopidx in xrange(stopy-stopx):
                load_lr[allidx-stopidx][4] = (stopy - stopidx)*\
                                                    cof_a + cof_b   
        
    return load_lr


# The next step is to put these load back to the dataframe so that
# the daily load could be calculated.
def put_lrload_todf(df_1min, load_lr_allvar):
    
    # Change load_lr_allvar back to dictionary to speed up the calculation
    amo_lr_load = list(load_lr_allvar[0])
    no23_lr_load = list(load_lr_allvar[1])    
    tn_lr_load = list(load_lr_allvar[2])
    tp_lr_load = list(load_lr_allvar[3])
    op_lr_load = list(load_lr_allvar[4])
    sedi_lr_load = list(load_lr_allvar[5])  
    
    for amollidx in xrange(len(amo_lr_load)):
        amo_lr_load[amollidx] = (amo_lr_load[amollidx][0],\
                amo_lr_load[amollidx][4])
        no23_lr_load[amollidx] = (no23_lr_load[amollidx][0],\
                no23_lr_load[amollidx][4])       
        tn_lr_load[amollidx] = (tn_lr_load[amollidx][0],\
                tn_lr_load[amollidx][4])
        tp_lr_load[amollidx] = (tp_lr_load[amollidx][0],\
                tp_lr_load[amollidx][4]) 
        op_lr_load[amollidx] = (amo_lr_load[amollidx][0],\
                op_lr_load[amollidx][4])
        sedi_lr_load[amollidx] = (sedi_lr_load[amollidx][0],\
                sedi_lr_load[amollidx][4]) 

                
    amo_lr_load = dict(amo_lr_load)
    no23_lr_load = dict(no23_lr_load)
    tn_lr_load = dict(tn_lr_load)
    tp_lr_load = dict(tp_lr_load)
    op_lr_load = dict(op_lr_load)
    sedi_lr_load = dict(sedi_lr_load)
        
    for dfidx in xrange(len(df_1min)):
        
        if pd.Timestamp(df_1min.index[dfidx]) in amo_lr_load:
            df_1min["AmmoniaLoad(mg/s)"][dfidx] = float(amo_lr_load[pd.Timestamp\
                                            (df_1min.index[dfidx])])*60.0
                                            
        if pd.Timestamp(df_1min.index[dfidx]) in no23_lr_load:                                            
            df_1min["NO3NO2Load(mg/s)"][dfidx] = float(no23_lr_load[pd.Timestamp\
                                            (df_1min.index[dfidx])])*60.0   
                                            
        if pd.Timestamp(df_1min.index[dfidx]) in tn_lr_load:                                            
            df_1min["TotalNLoad(mg/s)"][dfidx] = float(tn_lr_load[pd.Timestamp\
                                            (df_1min.index[dfidx])])*60.0
                                            
        if pd.Timestamp(df_1min.index[dfidx]) in tp_lr_load:                                            
            df_1min["TotalPLoad(mg/s)"][dfidx] = float(tp_lr_load[pd.Timestamp\
                                            (df_1min.index[dfidx])])*60.0
                                            
        if pd.Timestamp(df_1min.index[dfidx]) in op_lr_load:
            df_1min["OrthoPLoad(mg/s)"][dfidx] = float(op_lr_load[pd.Timestamp\
                                            (df_1min.index[dfidx])])*60.0
                                            
        if pd.Timestamp(df_1min.index[dfidx]) in sedi_lr_load:                                            
            df_1min["SedimentLoad(g/s)"][dfidx] = float(sedi_lr_load[pd.Timestamp\
                                            (df_1min.index[dfidx])])*60.0

    return df_1min
        
        
# Then, we need to group the data into daily.
# At this time, the load unit will need to be considered.
# The unit of load in current dataframe is mg/s for nutrient and g/s for
# sediment. I need to convert them to g/min and add them together.
def group_to_daily(df_1min):
    # We need at least 3 outputs:
    # 1. houlry average flow not total.
    # daily flow and load
    df_subdaily_flow = 0
    df_daily_load = 0

    df_subdaily_flow = df_1min.groupby(\
            pd.TimeGrouper(freq="30Min")).mean()
    df_daily_load = df_1min.groupby(pd.TimeGrouper(freq="1D")).sum()
    
    return df_subdaily_flow, df_daily_load
        
        
        
        
        
def writing_outfiles(df_subdaily_flow,\
                     df_daily_load,\
                     outf_flow_subdaily,\
                     outf_load_daily\
                     ):


    print("Write subdaily flow outputs")
    # Flow need to be converted from l/s to m3/s
    
    outfid_flow_subdaily = open(outf_flow_subdaily, "w")
    outfid_flow_subdaily.writelines("Date\tTime\tFlow_cms\n")
    
    for subdflidx in range(len(df_subdaily_flow.index)):
        # flow unit need to be convert from l/s to m3/s
        # 1 l/s = 0.001 m3/s      
        outfid_flow_subdaily.writelines("%s\t%s\t%10.5f\n"           \
            %(pd.to_datetime(df_subdaily_flow.index[subdflidx]).strftime("%Y%m%d"), \
            pd.to_datetime(df_subdaily_flow.index[subdflidx]).strftime("%H%M"), \
            df_subdaily_flow["Flow(l/s)"][subdflidx]/1000 \
            ))
    outfid_flow_subdaily.close()

    print("Write daily load outputs")
    # Flow need to be converted from l/s to m3/s
    
    outfid_load_daily = open(outf_load_daily, "w")
    outfid_load_daily.writelines("Date\tFlow_cms\tAmmo_kgday\tNO3NO2_kgday\tTN_kgday\tTP_kgday\tOP_kgday\tSEDIkgday\n")
    
    for dflidx in range(len(df_daily_load.index)):
        # Change nan values into 0
        if pd.isnull(df_daily_load["Flow(l/s)"][dflidx]):
            df_daily_load["Flow(l/s)"][dflidx] = 0.0
        if pd.isnull(df_daily_load["AmmoniaLoad(mg/s)"][dflidx]):
            df_daily_load["AmmoniaLoad(mg/s)"][dflidx] = 0.0
        if pd.isnull(df_daily_load["NO3NO2Load(mg/s)"][dflidx]):
            df_daily_load["NO3NO2Load(mg/s)"][dflidx] = 0.0
        if pd.isnull(df_daily_load["TotalNLoad(mg/s)"][dflidx]):
            df_daily_load["TotalNLoad(mg/s)"][dflidx] = 0.0
        if pd.isnull(df_daily_load["TotalPLoad(mg/s)"][dflidx]):
            df_daily_load["TotalPLoad(mg/s)"][dflidx] = 0.0
        if pd.isnull(df_daily_load["OrthoPLoad(mg/s)"][dflidx]):
            df_daily_load["OrthoPLoad(mg/s)"][dflidx] = 0.0
        if pd.isnull(df_daily_load["SedimentLoad(g/s)"][dflidx]):
            df_daily_load["SedimentLoad(g/s)"][dflidx] = 0.0
        
        # Unit conversion was included to kg or tons for nutrient and sedi
        outfid_load_daily.writelines("%s\t%f\t%f\t%f\t%f\t%f\t%f\t%f\n" \
            %(pd.to_datetime(df_daily_load.index[dflidx]).strftime("%Y%m%d %H%M"), \
            df_daily_load["Flow(l/s)"][dflidx], \
            df_daily_load["AmmoniaLoad(mg/s)"][dflidx]/1000000.0,\
            df_daily_load["NO3NO2Load(mg/s)"][dflidx]/1000000.0,\
            df_daily_load["TotalNLoad(mg/s)"][dflidx]/1000000.0,\
            df_daily_load["TotalPLoad(mg/s)"][dflidx]/1000000.0,\
            df_daily_load["OrthoPLoad(mg/s)"][dflidx]/1000000.0,\
            df_daily_load["SedimentLoad(g/s)"][dflidx]/1000.0\
                            ))
    outfid_load_daily.close()
#
#
################################################################################

# Due to the mismatching of some time between flow and conc data, I here
# put the values of next minutes into the missing minutes. The original
# flow data was in 2 minutes interval, I here used a 1 min dataframe. 
# The gaps will be filled using the next minutes. 
# At the same time, the load will be calculated

# Calling functions
#############################################################################

print("Reading flow data")
lrf_flow = read_flow(inf_flow)
print("Finished reading flow data")

print("Reading concentration data")
lrf_conc = read_conc(inf_conc)
print("Finished reading conc data")


df_1min_columns = ["Flow(l/s)", "Ammonia(ppm)", "NO3NO2(ppm)",\
                        "TotalN(ppm)", "TotalP(ppm)", "OrthoP(ppm)",\
                        "Sediment(g/L)",\
                        "AmmoniaLoad(mg/s)",\
                        "NO3NO2Load(mg/s)",\
                        "TotalNLoad(mg/s)",\
                        "TotalPLoad(mg/s)",\
                        "OrthoPLoad(mg/s)",\
                        "SedimentLoad(g/s)"]  

def main_eventload(yridx):
    
    # Defining parameters
    df_start_time = "%i-05-13" % (yridx)
    df_end_time = "%i-05-18"   % (yridx)
    print(df_start_time)
    print(df_end_time)
    
    print("Generating time frames")
    df_1min = generate_timeframe(df_start_time, df_end_time)
    print("Finished generating timeframe")
    
    print("Putting flow and concentration into timeframe")
    df_1min = put_flow_conc_todataframe(df_1min, lrf_flow, lrf_conc)
    print("Finished putting flow and concentration into timeframe")

    print("Filling data with next minutes")
    df_1min, amo_load_list, no23_load_list\
        , tn_load_list, tp_load_list, op_load_list\
        , sedi_load_list = fill_df_1min_nextmin(df_1min)
    print("Finished filling data with next minutes")
    
    print("Linear regression")
    load_list_allvar = [amo_load_list, no23_load_list\
        , tn_load_list, tp_load_list, op_load_list\
        , sedi_load_list]
    load_lr_allvar = [0]*len(load_list_allvar)
    for lridx in xrange(len(load_list_allvar)):
        load_lr = 0
        load_lr = linear_regression(load_list_allvar[lridx])
        load_lr_allvar[lridx] = list(load_lr)

    print("Putting interpolated load back to dataframe")
    df_1min = put_lrload_todf(df_1min, load_lr_allvar)
    print("Finished putting interpolated load back to dataframe")

    print("Summarizing load data to daily")
    df_subdaily_flow, df_daily_load = group_to_daily(df_1min)
    print("Finished summarizing load data to daily")

    outf_flow_subdaily = r"%s/%s/%s_FLOW_%s_%i.HLY" % (\
                    infd_flow, site_name, site_name,\
                    subdaily_freq, yridx)
    
    outf_load_daily = r"%s/%s/%s_LOAD_%i.DLY" % (\
                    outfd_load_daily, site_name, site_name, yridx)

    print("Writing output")
    writing_outfiles(df_subdaily_flow,\
                     df_daily_load,\
                     outf_flow_subdaily,\
                     outf_load_daily)
    print("Program run completed")


# The data will be for each year during only the growing season.
# And this will be running in parallel processing
import multiprocessing

if __name__ == '__main__':
#    jobs = []
#    for yrid in range(2009, 2015, 1):
#        p = multiprocessing.Process(target=main_eventload, args=(yrid,))
#        jobs.append(p)
#        p.start()
    #!year_list = np.arange(2009, 2015, 1)
    # I will simulate for this period
    try:
        pool = multiprocessing.Pool(7)
        pool.map(main_eventload, range(2009, 2015, 1))
    finally:
        pool.close()
        pool.join()
   

#print("Filling data with next minutes")
#df_1min, amo_load_list, no23_load_list\
#    , tn_load_list, tp_load_list, op_load_list\
#    , sedi_load_list = fill_df_1min_nextmin(df_1min)
#print("Finished filling data with next minutes")
#
#print("Linear regression")
#load_list_allvar = [amo_load_list, no23_load_list\
#    , tn_load_list, tp_load_list, op_load_list\
#    , sedi_load_list]
#load_lr_allvar = [0]*len(load_list_allvar)
#for lridx in xrange(len(load_list_allvar)):
#    load_lr = 0
#    load_lr = linear_regression(load_list_allvar[lridx])
#    load_lr_allvar[lridx] = list(load_lr)
#
#print("Putting interpolated load back to dataframe")
#df_1min = put_lrload_todf(df_1min, load_lr_allvar)
#print("Finished putting interpolated load back to dataframe")
#
#print("Summarizing load data to daily")
#df_daily_load = group_to_daily(df_1min)
#print("Finished summarizing load data to daily")
#
#print("Writing output")
#writing_outfiles(df_daily_load)
#print("Program run completed")
