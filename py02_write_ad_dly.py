"""
This script was created by Qingyu Feng
on Apr 12, 2016.

"""

# Environmental settings:

# for test file existance
import os
# 3. For resample the data points
import pandas as pd
import numpy as np

# Input files and folders:
###############################################################################
# Input folder names
site_name = "AD" 

infd_var = [0]*5
infd_var[0] = "dl01prcp"
infd_var[1] = "dl02temp"
infd_var[2]   = "dl03rh"
infd_var[3]   = "dl04windspeed"
infd_var[4]   = "dl05solar"

if not os.path.isdir(infd_var[0]): print("Check prcp folder")
if not os.path.isdir(infd_var[1]): print("Check temp folder")
if not os.path.isdir(infd_var[2]): print("Check rh folder")
if not os.path.isdir(infd_var[3]): print("Check windspeed folder")
if not os.path.isdir(infd_var[4]): print("Check solar radiation folder")
    
infd_garrett = "dl09_ncdc_garrett"
if not os.path.isdir(infd_garrett): print("Check garrett folder")

garrett = [0]*2
garrett[0] = r"%s/garrett1" %(infd_garrett)
garrett[1] = r"%s/garrett2" %(infd_garrett)

df_start_time = "2007-01-01"
df_end_time = "2014-12-31"

# Output folder namess
outfd_dlywp1 = r"out02_apex_dlywp1"
if not os.path.isdir(outfd_dlywp1): os.mkdir(outfd_dlywp1)

outfd_dlywp1_site = r"%s/%s" % (outfd_dlywp1, site_name)
if not os.path.isdir(outfd_dlywp1_site): os.mkdir(outfd_dlywp1_site)
print("Output folder for site %s is created!" %(site_name))

outf_dly = r"%s/%s.DLY" % (outfd_dlywp1_site, site_name)
outf_wp1 = r"%s/%s.WP1" % (outfd_dlywp1_site, site_name)

# Input file names based folder
inf_daily = [0] * 5
inf_daily[0] = r"%s/%s/%s_zf.DLY"  %(infd_var[0], site_name, site_name)
inf_daily[1] = r"%s/%s/%s_ntf.DLY" %(infd_var[1], site_name, site_name)
inf_daily[2] = r"%s/%s/%s_ntf.DLY" %(infd_var[2], site_name, site_name)
inf_daily[3] = r"%s/%s/%s_ntf.DLY" %(infd_var[3], site_name, site_name)
inf_daily[4] = r"%s/%s/%s_ntf.DLY" %(infd_var[4], site_name, site_name)

print(inf_daily)
inf_monthly = [0] * 5
inf_monthly[0] = r"%s/%s/%s_zf.MLY"  %(infd_var[0], site_name, site_name)
inf_monthly[1] = r"%s/%s/%s_ntf.MLY" %(infd_var[1], site_name, site_name)
inf_monthly[2] = r"%s/%s/%s_ntf.MLY" %(infd_var[2], site_name, site_name)
inf_monthly[3] = r"%s/%s/%s_ntf.MLY" %(infd_var[3], site_name, site_name)
inf_monthly[4] = r"%s/%s/%s_ntf.MLY" %(infd_var[4], site_name, site_name)

# Output file names
outf_dly = r"%s/%s.DLY" %(outfd_dlywp1_site, site_name)
outf_wp1 = r"%s/%s.WP1" %(outfd_dlywp1_site, site_name)

## Functions
###############################################################################
def read_garrett(garrett):
    
    print("Reading %s" %(garrett[0]))
    if not os.path.isfile(garrett[0]):
        print("%s does not exists! Please check!!!" %(garrett[0]))
        
    lrf_garrett1 = 0
    lrf_garrett2 = 0
    
    rf_garrett1 = open(garrett[0], "r")
    lrf_garrett1 = rf_garrett1.readlines()
    rf_garrett1.close()
    
    rf_garrett2 = open(garrett[1], "r")
    lrf_garrett2 = rf_garrett2.readlines()
    rf_garrett2.close()    
    
    del lrf_garrett1[0]
    del lrf_garrett2[0]
    
    for lrfidx in xrange(len(lrf_garrett1)):

        lrf_garrett1[lrfidx] = lrf_garrett1[lrfidx].split("\t")  
        for ddidx in lrf_garrett1[lrfidx]:
            if ddidx == "unknown":
                print(ddidx)
        
        lrf_garrett1[lrfidx][-1] = lrf_garrett1[lrfidx][-1][:-1]
        
        del lrf_garrett1[lrfidx][:-4]
        lrf_garrett1[lrfidx][0] = pd.Timestamp(lrf_garrett1[lrfidx][0])
        lrf_garrett1[lrfidx] = (lrf_garrett1[lrfidx][0], \
                                lrf_garrett1[lrfidx][1:])
    
    for lrf2idx in xrange(len(lrf_garrett2)):

        lrf_garrett2[lrf2idx] = lrf_garrett2[lrf2idx].split("\t")  
        for ddidx in lrf_garrett2[lrf2idx]:
            if ddidx == "unknown":
                print(ddidx)
        lrf_garrett2[lrf2idx][-1] = lrf_garrett2[lrf2idx][-1][:-1]
        del lrf_garrett2[lrf2idx][:-4]
        lrf_garrett2[lrf2idx][0] = pd.Timestamp(lrf_garrett2[lrf2idx][0])

        lrf_garrett2[lrf2idx] = (lrf_garrett2[lrf2idx][0],\
                                lrf_garrett2[lrf2idx][1:])
    # Convert the data structure to dictionary for quicker search                            
    lrf_garrett1 = dict(lrf_garrett1)
    lrf_garrett2 = dict(lrf_garrett2)
    
    return lrf_garrett1, lrf_garrett2



def read_daily(siteidx):
    
    print("Reading %s" %(inf_daily[daysiteidx]))
    if not os.path.isfile(inf_daily[daysiteidx]):
        print("%s does not exists! Please check!!!" %(inf_daily[daysiteidx]))
        
    lrf_var_daily = 0
    
    rf_var_daily = open(inf_daily[daysiteidx], "r")
    lrf_var_daily = rf_var_daily.readlines()
    rf_var_daily.close()
    
    for lrfidx in range(len(lrf_var_daily)):

        lrf_var_daily[lrfidx] = lrf_var_daily[lrfidx].split("\t")    
        lrf_var_daily[lrfidx][-1] = lrf_var_daily[lrfidx][-1][:-1]
        lrf_var_daily[lrfidx][0] = pd.Timestamp(lrf_var_daily[lrfidx][0])
        lrf_var_daily[lrfidx] = (lrf_var_daily[lrfidx][0], lrf_var_daily[lrfidx][1:])
    lrf_var_daily = dict(lrf_var_daily)
    
    return lrf_var_daily

def read_monthly(monsiteidx):
    
    print("Reading %s" %(inf_monthly[monsiteidx]))
    if not os.path.isfile(inf_monthly[monsiteidx]):
        print("%s does not exists! Please check!!!" %(inf_monthly[monsiteidx]))
        
    lrf_var_monthly = 0
    
    rf_var_monthly = open(inf_monthly[monsiteidx], "r")
    lrf_var_monthly = rf_var_monthly.readlines()
    rf_var_monthly.close()
    
    for lrfidx in range(len(lrf_var_monthly)):

        lrf_var_monthly[lrfidx] = lrf_var_monthly[lrfidx].split("\t")    
        lrf_var_monthly[lrfidx][-1] = lrf_var_monthly[lrfidx][-1][:-1]

        lrf_var_monthly[lrfidx] = (lrf_var_monthly[lrfidx][0], lrf_var_monthly[lrfidx][1:])
        
    lrf_var_monthly = dict(lrf_var_monthly)
    
    return lrf_var_monthly




def generate_timeframe(df_start_time, df_end_time, infd_var):
    
    print(infd_var)
    df_daily = 0    
    df_monthly = 0    

    df_daily_index = pd.date_range(df_start_time,\
                                    df_end_time,\
                                    freq = "1D")
    # Two columns need to be included:
    # 1. The precipitation data.
    # 2. The time interval. For subdaily simulation, the time fraction
    # need to be calculated as fraction of hours. 
    # This fraction of hours will be calculated after grouping.
    df_daily_columns = ["dl01prcp", "dl02temp_max", "dl02temp_min",\
                        "dl03rh", "dl04windspeed", "dl05solar",\
                         "garrett1_prcp", "garrett1_tmax", "garrett1_tmin",\
                         "garrett2_prcp", "garrett2_tmax", "garrett2_tmin"\
                        ]
       
    
    df_daily =  pd.DataFrame(np.zeros((len(df_daily_index), \
                                len(df_daily_columns))), \
                                columns = df_daily_columns, \
                                index = df_daily_index,\
                                dtype=float)
    for dfcidx in df_daily_columns:
        df_daily[dfcidx] = np.nan

    df_monthly_index = np.arange(1, 13, 1)
    # Two columns need to be included:
    # 1. The precipitation data.
    # 2. The time interval. For subdaily simulation, the time fraction
    # need to be calculated as fraction of hours. 
    # This fraction of hours will be calculated after grouping.
    df_monthly_columns = ["avg_mon_tmax", "avg_mon_tmin",\
                            "mon_avg_std_daily_tmax","mon_avg_std_daily_tmin",\
                        "avg_mon_prcp", "mon_std_daily_prcp", \
                        "mon_skew_daily_prcp",\
                         "avg_mon_solar", "avg_mon_rh", "avg_mon_ws"\
                        ]
    
    df_monthly =  pd.DataFrame(np.zeros((len(df_monthly_index), \
                                len(df_monthly_columns))), \
                                columns = df_monthly_columns, \
                                index = df_monthly_index,\
                                dtype=float)
    for dfcidx in df_monthly_columns:
        df_monthly[dfcidx] = np.nan
    
    return df_daily, df_monthly


def put_data_to_dataframe(df_daily, df_monthly, lrf_var_daily_all, infd_var,\
                            lrf_garrett1, lrf_garrett2, lrf_var_monthly_all):

    for dfidx in df_daily.index:
        if dfidx in lrf_var_daily_all[0]:
            df_daily.set_value(dfidx, "dl01prcp",\
                        float(lrf_var_daily_all[0][dfidx][0]))
        if dfidx in lrf_var_daily_all[1]:
            df_daily.set_value(dfidx, "dl02temp_max",\
                        float(lrf_var_daily_all[1][dfidx][0]))
            df_daily.set_value(dfidx, "dl02temp_min",\
                        float(lrf_var_daily_all[1][dfidx][1]))
        if dfidx in lrf_var_daily_all[2]:
            # RH need to be converted from % to fraction
            df_daily.set_value(dfidx, "dl03rh",\
                        float(lrf_var_daily_all[2][dfidx][0])/100)
        if dfidx in lrf_var_daily_all[3]:
            # Unit of windspeed need to be converted from
            # currently km/h to m/s
            df_daily.set_value(dfidx, "dl04windspeed",\
                        float(lrf_var_daily_all[3][dfidx][0])/3.6)
        if dfidx in lrf_var_daily_all[4]:
            # Unit of solar radiation need to be converted from
            # currently kWh/m2/day to Mj/m2/day 
            df_daily.set_value(dfidx, "dl05solar",\
                        float(lrf_var_daily_all[4][dfidx][0])*3.6)
        if dfidx in lrf_garrett1:
            # Unit of solar radiation need to be converted from
            # currently kWh/m2/day to Mj/m2/day 
            df_daily.set_value(dfidx, "garrett1_prcp",\
                        float(lrf_garrett1[dfidx][0]))
            df_daily.set_value(dfidx, "garrett1_tmax",\
                        float(lrf_garrett1[dfidx][1]))               
            df_daily.set_value(dfidx, "garrett1_tmin",\
                        float(lrf_garrett1[dfidx][2]))               
        if dfidx in lrf_garrett2:
            # Unit of solar radiation need to be converted from
            # currently kWh/m2/day to Mj/m2/day 
            df_daily.set_value(dfidx, "garrett2_prcp",\
                        float(lrf_garrett2[dfidx][0]))
            df_daily.set_value(dfidx, "garrett2_tmax",\
                        float(lrf_garrett2[dfidx][1]))               
            df_daily.set_value(dfidx, "garrett2_tmin",\
                        float(lrf_garrett2[dfidx][2]))                 
               
        # The precipitation and temperature data need to be filled with
        # garrett data if it is zero.
        if pd.isnull(df_daily["dl01prcp"][dfidx]):
            if pd.notnull(df_daily["garrett1_prcp"][dfidx]):
                df_daily["dl01prcp"][dfidx] = df_daily["garrett1_prcp"][dfidx]
            elif pd.notnull(df_daily["garrett2_prcp"][dfidx]):
                df_daily["dl01prcp"][dfidx] = df_daily["garrett2_prcp"][dfidx]
            else:
                df_daily["dl01prcp"][dfidx] = df_daily["dl01prcp"][dfidx]
                
        if pd.isnull(df_daily["dl02temp_max"][dfidx]):
            if pd.notnull(df_daily["garrett1_tmax"][dfidx]):
                df_daily["dl02temp_max"][dfidx] = df_daily["garrett1_tmax"][dfidx]
            elif pd.notnull(df_daily["garrett2_tmax"][dfidx]):
                df_daily["dl02temp_max"][dfidx] = df_daily["garrett2_tmax"][dfidx]
            else:
                df_daily["dl02temp_max"][dfidx] = df_daily["dl02temp_max"][dfidx]
                
        if pd.isnull(df_daily["dl02temp_min"][dfidx]):
            if pd.notnull(df_daily["garrett1_tmin"][dfidx]):
                df_daily["dl02temp_min"][dfidx] = df_daily["garrett1_tmin"][dfidx]
            elif pd.notnull(df_daily["garrett2_tmin"][dfidx]):
                df_daily["dl02temp_min"][dfidx] = df_daily["garrett2_tmin"][dfidx]
            else:
                df_daily["dl02temp_min"][dfidx] = df_daily["dl02temp_min"][dfidx]
         
    # Monthly data into framework
    for mondfidx in df_monthly.index:
        
        if str(mondfidx) in lrf_var_monthly_all[1]:
            df_monthly.set_value(mondfidx, "avg_mon_tmax",\
                        float(lrf_var_monthly_all[1][str(mondfidx)][0]))
            df_monthly.set_value(mondfidx, "avg_mon_tmin",\
                        float(lrf_var_monthly_all[1][str(mondfidx)][1]))
            df_monthly.set_value(mondfidx, "mon_avg_std_daily_tmax",\
                        float(lrf_var_monthly_all[1][str(mondfidx)][2]))
            df_monthly.set_value(mondfidx, "mon_avg_std_daily_tmin",\
                        float(lrf_var_monthly_all[1][str(mondfidx)][3]))
        
        if str(mondfidx) in lrf_var_monthly_all[0]:
            df_monthly.set_value(mondfidx, "avg_mon_prcp",\
                        float(lrf_var_monthly_all[0][str(mondfidx)][0]))
            df_monthly.set_value(mondfidx, "mon_std_daily_prcp",\
                        float(lrf_var_monthly_all[0][str(mondfidx)][1]))
            df_monthly.set_value(mondfidx, "mon_skew_daily_prcp",\
                        float(lrf_var_monthly_all[0][str(mondfidx)][2]))                        
                        
        if str(mondfidx) in lrf_var_monthly_all[4]:
            df_monthly.set_value(mondfidx, "avg_mon_solar",\
                        float(lrf_var_monthly_all[4][str(mondfidx)][0])*3.6)
                        
        if str(mondfidx) in lrf_var_monthly_all[2]:
            df_monthly.set_value(mondfidx, "avg_mon_rh",\
                        float(lrf_var_monthly_all[2][str(mondfidx)][0]))                        
                        
        if str(mondfidx) in lrf_var_monthly_all[3]:
            df_monthly.set_value(mondfidx, "avg_mon_ws",\
                        float(lrf_var_monthly_all[3][str(mondfidx)][0]))                        
         
    return df_daily, df_monthly


def fill_missing_rhsrws(df_daily):
    
    # According to the count function, there are no missing data for temperature.
    # 31 missing data for rh, ws and solar.
    # Since this is flat area and not much changed, I will currently fill the 
    # missing data for this period using the former day value. This
    # is based on the assumption that the changes based on daily is not
    # large for solar radiation. It might be for relative humidity and 
    # wind speed. Since we do not have other options. I will go with 
    # this method and improve it if we got other data sources. 
    df_daily_filled_fd = pd.DataFrame(df_daily)
    
    for ddfidx in range(1, len(df_daily_filled_fd)):
        if (pd.isnull(df_daily_filled_fd["dl01prcp"][ddfidx]) or \
        df_daily_filled_fd["dl01prcp"][ddfidx] == -9999\
        ):
            df_daily_filled_fd["dl01prcp"][ddfidx] = 0
                        
        if (pd.isnull(df_daily_filled_fd["dl02temp_max"][ddfidx]) or \
        df_daily_filled_fd["dl02temp_max"][ddfidx] == -9999\
        ):
            print(df_daily_filled_fd["dl02temp_max"][ddfidx])
            print(df_daily_filled_fd["dl02temp_max"][ddfidx-1])

            df_daily_filled_fd["dl02temp_max"][ddfidx] =\
                        df_daily_filled_fd["dl02temp_max"][ddfidx-1] 
                        
        if (pd.isnull(df_daily_filled_fd["dl02temp_min"][ddfidx]) or \
        df_daily_filled_fd["dl02temp_min"][ddfidx] == -9999\
        ):
            df_daily_filled_fd["dl02temp_min"][ddfidx] =\
                        df_daily_filled_fd["dl02temp_min"][ddfidx-1] 
                        
        if (pd.isnull(df_daily_filled_fd["dl03rh"][ddfidx]) or \
        df_daily_filled_fd["dl03rh"][ddfidx] == -9999\
        ):
            df_daily_filled_fd["dl03rh"][ddfidx] =\
                        df_daily_filled_fd["dl03rh"][ddfidx-1] 
                        
        if (pd.isnull(df_daily_filled_fd["dl04windspeed"][ddfidx]) or \
        df_daily_filled_fd["dl04windspeed"][ddfidx] == -9999\
        ):
            df_daily_filled_fd["dl04windspeed"][ddfidx] =\
                        df_daily_filled_fd["dl04windspeed"][ddfidx-1]
                        
        if (pd.isnull(df_daily_filled_fd["dl05solar"][ddfidx]) or \
        df_daily_filled_fd["dl05solar"][ddfidx] == -9999\
        ):
            df_daily_filled_fd["dl05solar"][ddfidx] = \
                        df_daily_filled_fd["dl05solar"][ddfidx-1]
    
    return df_daily_filled_fd



def writing_outfiles(df_daily_filled_fd):
   
    print("Writing apex dly")
    outfid_dly = open(outf_dly, "w") 
    for dldidx in df_daily_filled_fd.index:
        outfid_dly.writelines("  %4i%4i%4i%6.2f%6.2f%6.2f%6.2f%6.2f%6.2f\n" \
            %(dldidx.year, dldidx.month, dldidx.day,\
                float(df_daily_filled_fd["dl05solar"][dldidx]),\
                float(df_daily_filled_fd["dl02temp_max"][dldidx]),\
                float(df_daily_filled_fd["dl02temp_min"][dldidx]),\
                float(df_daily_filled_fd["dl01prcp"][dldidx]),\
                float(df_daily_filled_fd["dl03rh"][dldidx]),\
                float(df_daily_filled_fd["dl04windspeed"][dldidx])\
                ))                
    outfid_dly.close()


#
# Calling Functions:
print("Reading data from NCDC garrett stations")
lrf_garrett1, lrf_garrett2 = read_garrett(garrett)
print("Finished reading data from NCDC garrett stations")

lrf_var_daily_all = [0]*len(inf_daily)
lrf_var_monthly_all = [0]*len(inf_monthly)

for daysiteidx in xrange(len(inf_daily)):
    lrf_var_daily = read_daily(daysiteidx)
    lrf_var_daily_all[daysiteidx] = lrf_var_daily       


for monsiteidx in xrange(len(inf_monthly)):
    lrf_var_monthly = read_monthly(monsiteidx)
    lrf_var_monthly_all[monsiteidx] = lrf_var_monthly
    

print(infd_var)
print("Generating time frames")
df_daily, df_monthly = generate_timeframe(df_start_time, df_end_time, infd_var)
print("Finished generating timeframe")


print("Putting solar radiation data into timeframe")
df_daily, df_monthly = put_data_to_dataframe(df_daily, df_monthly,\
                                            lrf_var_daily_all, infd_var,\
                                            lrf_garrett1, lrf_garrett2,\
                                            lrf_var_monthly_all)
print("Finished putting solar radiation into timeframe")
                                            
print("Filling daily data using the former day")                                            
df_daily_filled_fd = fill_missing_rhsrws(df_daily)                                     
print("Finished filling daily data using the former day")                                            
               
print("Writing output")
writing_outfiles(df_daily_filled_fd)
print("Finished writing output")

