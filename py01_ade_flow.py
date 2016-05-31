"""
This script was created by Qingyu Feng
on Mar 17, 2016.

 sd
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
site_name = "ADE" 

infd_flow = r"dl06flow"
infd_conc = r"dl07conc"
if not os.path.isdir(infd_flow): print("Observed data for flow does not exist")
if not os.path.isdir(infd_conc): print("Observed data for conc does not exist")

# I will simulate for this period
df_start_time = "2007-04-25 00:00:00"
df_end_time = "2014-12-31 23:59:59"

inf_flow = r"%s/%s/%s.orig" %(infd_flow, site_name, site_name)
inf_conc = r"%s/%s/%s.orig" %(infd_conc, site_name, site_name)

subdaily_freq = "30Min" # This will be the frequency that 
                            # this proram will  group by from a 
                            # 1min frequency. The parameter should be string.
                            # Examples include: "1H", "2D", "3M" for month.

outfd_load_daily = r"dl10_load_insjsites"
if not os.path.isdir(outfd_load_daily): os.mkdir(outfd_load_daily)

outf_flow_subdaily = r"%s/%s/%s_FLOW_%s.HLY" % (\
                infd_flow, site_name, site_name, subdaily_freq)

outf_flow_daily = r"%s/%s/%s_FLOW.DLY" % (\
                infd_flow, site_name, site_name)

outf_load_daily = r"%s/%s/%s_LOAD.DLY" % (\
                outfd_load_daily, site_name, site_name)

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



def generate_timeframe(lrf_flow, df_start_time, df_end_time):
  
    print(df_start_time)
    print(df_end_time)
    
    df_1min_index = pd.date_range(df_start_time,df_end_time, freq = "1min")
    
    df_1min_columns = ["Flow(l/s)", "Ammonia(ppm)", "NO3NO2(ppm)",\
                        "TotalN(ppm)", "TotalP(ppm)", "OrthoP(ppm)",\
                        "Sediment(g/L)"]
    df_1min =  pd.DataFrame(np.zeros((len(df_1min_index), 7)), \
                                columns = df_1min_columns, \
                                index = df_1min_index)   
    
    for dfcidx in df_1min_columns:
        df_1min[dfcidx] = np.nan
    
    
    return df_1min
    
    
    
def put_flow_conc_todataframe(df_1min, lrf_flow, lrf_conc):
    
    for dfidx in range(len(df_1min)):
#        print("before flow", pd.Timestamp(df_1min.index[dfidx]))
        if pd.Timestamp(df_1min.index[dfidx]) in lrf_flow:
            df_1min["Flow(l/s)"][dfidx] = lrf_flow[pd.Timestamp\
                                            (df_1min.index[dfidx])]
                
#        print
#        if pd.Timestamp(df_1min.index[dfidx]) in lrf_conc:
##            print(lrf_conc[pd.Timestamp(df_1min.index[dfidx]]))
#            df_1min["Ammonia(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
#                                            (df_1min.index[dfidx])][0])
#            df_1min["NO3NO2(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
#                                            (df_1min.index[dfidx])][1])                                           
#            df_1min["TotalN(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
#                                            (df_1min.index[dfidx])][2])
#            df_1min["TotalP(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
#                                            (df_1min.index[dfidx])][3])
#            df_1min["OrthoP(ppm)"][dfidx] = float(lrf_conc[pd.Timestamp\
#                                            (df_1min.index[dfidx])][4])
#            df_1min["Sediment(g/L)"][dfidx] = float(lrf_conc[pd.Timestamp\
#                                            (df_1min.index[dfidx])][5])
##        elif pd.Timestamp(df_1min.index[dfidx]) = 

        
    return df_1min
    
    
# The base have been created.
# Now, there will be two parallel processing:
# 1. Fill the no observation times with conc and flow from the former 
#    day observation unless there is a new observation.
# 2. Generate a list of times where flow and conc all exists. Then, export to
#    loadest model inputs.
def fill_df_1min_nextday(df_1min):
    
    # loadest input list:
    # Flow for the est.inp
    df_1min_filled_nd = pd.DataFrame.copy(df_1min)
#    print(ldiplst_flow)

#    ldiplst_flow[0] = [df_1min[1][0].strftime("%Y%m%d"), 
#                         df_1min[1][0].strftime("%H%M"),
#                        df_1min[1][1]]
    # The first value will be assigned because following code start calculation
    # from line 2.

    for dffidx in xrange(len(df_1min_filled_nd.index)-1):
        # The output will be at 1 mins.
        # If there is no flow data, the value for the next time will be assigned
        # The original unit of flow is liter per second
        # In loadest, the flow unit could be cubic feet per second.
        # The unit will be converted when the data are write into output
        
        if pd.isnull(df_1min_filled_nd["Flow(l/s)"][dffidx]):
            df_1min_filled_nd["Flow(l/s)"][dffidx] =\
                df_1min_filled_nd["Flow(l/s)"][dffidx+1]
            
        # At this time, before the conc was modified, a list of conc will need to
        # be created that include only the observed flow and conc.
        # Before concentration data was filled into the flow,
        # if there is a missing data, -9999 will also be converted.
        # The data in df_1min will be reserved for future analysis.
        # The first method was to fill it by the value of next day until
        # a value is encountered.
#        if df_1min_filled_nd["Flow(l/s)"][dffidx] >= 0.1:
#            point_count = 0
#            while pd.isnull(df_1min_filled_nd["Ammonia(ppm)"][dffidx+point_count]) and point_count <=1500:
#                point_count = point_count + 1
#            df_1min_filled_nd["Ammonia(ppm)"][dffidx] =\
#                    df_1min_filled_nd["Ammonia(ppm)"][dffidx+point_count]
#            print(df_1min_filled_nd.index[dffidx], \
#                df_1min_filled_nd["Ammonia(ppm)"][dffidx])
#
#        else:
#            df_1min_filled_nd["Ammonia(ppm)"][dffidx] = 0
#        if pd.notnull(df_1min_filled_nd["Ammonia(ppm)"][dffidx]):
#            point_count = 0
#            while df_1min_filled_nd["Flow(l/s)"][dffidx-point_count] != 0 and\
#                pd.isnull(df_1min_filled_nd["Ammonia(ppm)"][dffidx]):
#                point_count = point_count + 1
#                df_1min_filled_nd["Ammonia(ppm)"][dffidx-point_count] =\
#                    df_1min_filled_nd["Ammonia(ppm)"][dffidx]
                

            



#        if df_1min_filled_nd["Flow(l/s)"][dffidx] == 0:
#            df_1min_filled["Ammonia(ppm)"][dffidx] = 0
#            df_1min_filled["NO3NO2(ppm)"][dffidx] = 0 
#            df_1min_filled["TotalN(ppm)"][dffidx] = 0
#            df_1min_filled["TotalP(ppm)"][dffidx] = 0
#            df_1min_filled["OrthoP(ppm)"][dffidx] = 0
#            df_1min_filled["Sediment(g/L)"][dffidx] = 0
#        else:
#            try:
#                n = 1
#                while pd.isnull(df_1min_filled["Ammonia(ppm)"][dffidx]):
#                    n = n+1
#                    print
                    
                    
            
            
#            
#                df_1min_filled["Ammonia(ppm)"][dffidx-1]
#            
#        if (df_1min_filled["NO3NO2(ppm)"][dffidx] == 0) or \
#            (df_1min_filled["NO3NO2(ppm)"][dffidx] == -9999.0):
#            df_1min_filled["NO3NO2(ppm)"][dffidx] = df_1min_filled["NO3NO2(ppm)"][dffidx-1]
#            
#        if (df_1min_filled["TotalN(ppm)"][dffidx] == 0) or \
#            (df_1min_filled["TotalN(ppm)"][dffidx] == -9999.0):
#            df_1min_filled["TotalN(ppm)"][dffidx] = df_1min_filled["TotalN(ppm)"][dffidx-1]
#            
#        if (df_1min_filled["TotalP(ppm)"][dffidx] == 0) or \
#            (df_1min_filled["TotalP(ppm)"][dffidx] == -9999.0):
#            df_1min_filled["TotalP(ppm)"][dffidx] = df_1min_filled["TotalP(ppm)"][dffidx-1]
#            
#        if (df_1min_filled["OrthoP(ppm)"][dffidx] == 0) or \
#            (df_1min_filled["OrthoP(ppm)"][dffidx] == -9999.0):
#            df_1min_filled["OrthoP(ppm)"][dffidx] = df_1min_filled["OrthoP(ppm)"][dffidx-1]  
#            
#        if (df_1min_filled["Sediment(g/L)"][dffidx] == 0) or \
#            (df_1min_filled["Sediment(g/L)"][dffidx] == -9999.0):
#            df_1min_filled["Sediment(g/L)"][dffidx] = df_1min_filled["Sediment(g/L)"][dffidx-1]
#            
##        print(ldiplst_flow.iloc[dffidx])
        
    # Now, the load need to be calculated. Dataframe column could be added directly.
#    df_1min_filled.insert(7, "Ammonia(kg)", \
#                            df_1min_filled["Ammonia(ppm)"] \
#                            * df_1min_filled["Flow_l/s"] \
#                            * 60/1000000)
#                            
#    df_1min_filled.insert(8, "NO3+N2(kg)", \
#                            df_1min_filled["NO3+N2(ppm)"] \
#                            * df_1min_filled["Flow_l/s"] \
#                            * 60/1000000)
#                            
#    df_1min_filled.insert(9, "Total N(kg)", \
#                            df_1min_filled["Total N(ppm)"] \
#                            * df_1min_filled["Flow_l/s"] \
#                            * 60/1000000)
#
#    df_1min_filled.insert(10, "Total P(kg)", \
#                            df_1min_filled["Total P(ppm)"] \
#                            * df_1min_filled["Flow_l/s"] \
#                            * 60/1000000)
#                            
#    df_1min_filled.insert(11, "Ortho P(kg)", \
#                            df_1min_filled["Ortho P(ppm)"] \
#                            * df_1min_filled["Flow_l/s"] \
#                            * 60/1000000)
#                            
#    df_1min_filled.insert(12, "Sediment(kg)", \
#                            df_1min_filled["Sediment(g/L)"] \
#                            * df_1min_filled["Flow_l/s"] \
#                            * 60/1000)
                          
#            ldiplst_flow[dffidx] = [df_1min[dffidx+1][0].strftime("%Y%m%d"), 
#                                     df_1min[dffidx+1][0].strftime("%H%M"),
#                                    df_1min[dffidx+1][1]]
#        else:
#            ldiplst_flow[dffidx] = [df_1min[dffidx+1][0].strftime("%Y%m%d"), 
#                                     df_1min[dffidx+1][0].strftime("%H%M"),
#                                    df_1min[dffidx][1]]
#    print(df_1min_filled["Ammonia(kg)"])
    return df_1min_filled_nd
        
        
        
def loadest_input_calinp(df_1min_filled, lrf_conc):
    
    # Discrete observation at different days
    # There could be more than one variables (constitute in loadest)
    # Missing data for variable could be set as -9999.0
    
    # To generate a list, the flow should not be 0
    # In addition, there should be an observation close to the data of flow.
    # Generally, there is 2 mins flow and concentration at discrete time.
    # The rule of selection for conc for a specific date will be:
    # If there is conc for the flow time, it will be assinged.
    # If there is not one, the one close to a flow data will be assined.
    # column: dt, flow, ammonia, NO3+NO2, TN, TP, OP, TSS
    for lrfcidx in sorted(lrf_conc.keys()):
        lrf_conc[lrfcidx].append(df_1min_filled.loc[lrfcidx][0])
        
    return lrf_conc
        
    
        
def group_daily_subdaily(df_1min_filled_nd):

    # We need at least 3 outputs:
    # 1. houlry average flow not total.
    # daily flow and load
    df_subdaily_flow = df_1min_filled_nd.groupby(pd.TimeGrouper(freq="30Min")).mean()
#    print(df_hourly_flow)

    df_daily_flow = df_1min_filled_nd.groupby(pd.TimeGrouper(freq="1D")).mean()
#    print(df_daily_flow)

#    df_daily_load = df_1min_filled.groupby(pd.TimeGrouper(freq="1D")).sum()
#    print(df_daily_flow_load) 
    
    
    
    return df_subdaily_flow, df_daily_flow#, df_daily_load



def writing_outfiles():
    
    # There are two purpose of this function
    # 1. convert units before writing
    # 2. write output files for different purpose
    
#    print("Write loadest inputs")
#    # get template
#    inf_calib = open(loadest_template_calib, "r")
#    tlinf_calib = inf_calib.readlines()
#    inf_calib.close()
#    
#    del tlinf_calib[13:]
#    
#    
#    tlinf_calib[11] = "#CDATE\tCTIME\tFLOWcfs\tAmmonia_ppm\tNO3NO2_ppm\tTN_ppm\tTP_ppm\tOrthoP_ppm\tSediment_mgperl\n"
#    # write calib
#    outf_calib = open(outf_loadest_calib, "w")
#    for tlinfidx in tlinf_calib:
#        outf_calib.writelines(tlinfidx)
#    
#    lrfc_keyindex = sorted(lrf_conc.keys())
#    for lrfckidx in range(len(lrfc_keyindex)):
##        print(pd.to_datetime(lrfc_keyindex[lrfckidx]).strftime("%Y%m%d")) 
##        print(lrf_conc[lrfc_keyindex[lrfckidx]])
#        outf_calib.writelines("%s\t%s\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%8.2f\t%8.2f\n"  \
#                               %(pd.to_datetime(lrfc_keyindex[lrfckidx]).strftime("%Y%m%d"), \
#                               pd.to_datetime(lrfc_keyindex[lrfckidx]).strftime("%H%M"), \
#                               lrf_conc[lrfc_keyindex[lrfckidx]][-1], \
#                               float(lrf_conc[lrfc_keyindex[lrfckidx]][0]), \
#                               float(lrf_conc[lrfc_keyindex[lrfckidx]][1]), \
#                               float(lrf_conc[lrfc_keyindex[lrfckidx]][2]), \
#                               float(lrf_conc[lrfc_keyindex[lrfckidx]][3]), \
#                               float(lrf_conc[lrfc_keyindex[lrfckidx]][4]), \
#                               float(lrf_conc[lrfc_keyindex[lrfckidx]][5]) \
#                               ))
#    outf_calib.close()         
#    
#    
#    # Write est files:
#    inf_est = open(loadest_template_est, "r")
#    tlinf_est = inf_est.readlines()
#    inf_est.close()
#    
#    del tlinf_est[19:]
#    
#    # write est
#    outf_est = open(outf_loadest_est, "w")
#    for tlinfidx in tlinf_est:
#        outf_est.writelines(tlinfidx)
#    
#    for ldfidx in range(len(df_1min_filled.index)):
#        # flow unit need to be convert from l/s to ft3/s
#        # 1 l/s = 0.0353147 ft3/s
##        print(df_1min_filled.index[ldfidx])
##        print(df_1min_filled["Flow_l/s"])
#        
#        outf_est.writelines("%s\t%s\t%10.5f\n"           \
#                            %(pd.to_datetime(df_1min_filled.index[ldfidx]).strftime("%Y%m%d"), \
#                            pd.to_datetime(df_1min_filled.index[ldfidx]).strftime("%H%M"), \
#                            df_1min_filled["Flow_l/s"][ldfidx]*0.0353147 \
#                            ))
#    outf_est.close()

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
    
  
    print("Write daily flow outputs")
    # Flow need to be converted from l/s to m3/s
    
    outfid_flow_daily = open(outf_flow_daily, "w")
    outfid_flow_daily.writelines("Date\tFlow_cms\n")
    
    for dflidx in range(len(df_daily_flow.index)):
        # flow unit need to be convert from l/s to m3/s
        # 1 l/s = 0.001 m3/s      
        outfid_flow_daily.writelines("%s\t%10.5f\n"           \
                            %(pd.to_datetime(df_daily_flow.index[dflidx]).strftime("%Y%m%d"), \
                            df_daily_flow["Flow(l/s)"][dflidx]/1000 \
                            ))
    outfid_flow_daily.close()


#    print("Write daily load outputs")
#    outfid_daily_load = open(outf_load_daily, "w")
#    outfid_daily_load.writelines("Date\tAmmo_kgday\tNO3NO2_kgday\tTN_kgday\tTP_kgday\tOP_kgday\tSEDIkgday\n")
#    for dldidx in range(len(df_daily_load.index)):
#        outfid_daily_load.writelines("%s\t%f\t%f\t%f\t%f\t%f\t%f\n" \
#                                    %(pd.to_datetime(df_daily_load.index[dldidx]).strftime("%Y%m%d"), \
#                                    float(df_daily_load["Ammonia(kg)"][dldidx]), \
#                                    float(df_daily_load["NO3+N2(kg)"][dldidx]), \
#                                    float(df_daily_load["Total N(kg)"][dldidx]), \
#                                    float(df_daily_load["Total P(kg)"][dldidx]), \
#                                    float(df_daily_load["Ortho P(kg)"][dldidx]),\
#                                    float(df_daily_load["Sediment(kg)"][dldidx]) \
#                                ))
#    outfid_daily_load.close()
#    

###############################################################################
### Calling functions
print("Reading flow data")
lrf_flow = read_flow(inf_flow)
print("Finished reading flow data")

print("Reading concentration data")
lrf_conc = read_conc(inf_conc)
print("Finished reading conc data")

print("Generating time frames")
df_1min = generate_timeframe(lrf_flow, df_start_time, df_end_time)
print("Finished generating timeframe")

print("Putting flow and concentration into timeframe")
df_1min = put_flow_conc_todataframe(df_1min, lrf_flow, lrf_conc)
print("Finished putting flow and concentration into timeframe")

print("Filling missing data for flow and conc in dataframe")
df_1min_filled_nd = fill_df_1min_nextday(df_1min)
print("Fnished filling missing data for flow and conc in dataframe")

##print("Generating calib.inp files for loadest")
##lrf_conc = loadest_input_calinp(df_1min_filled, lrf_conc)
##
print("Summarizing flow and concentration data")
df_subdaily_flow, df_daily_flow = group_daily_subdaily(df_1min_filled_nd)

print("Writing output")
writing_outfiles()
print("Program run completed")
