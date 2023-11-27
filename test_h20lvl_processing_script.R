#TEST SCRIPT TO IMPORT AND ANALYZE WETLAND DATA

.libPaths("C:/Users/stribling.stuber/documents/R/win-library/4.2")

# library(IRanges)
library(tidyverse)
library(janitor)
library(padr)
library(RcppRoll)
library(zoo)
# library(fuzzyjoin)


# list filepath to main dataset
hobo.data.fp <- "R:/ltr_wetlands/main_project/data/water_budget/hoboFiles/raw data/202105_May-2021"

manual.levels.fp <- "R:/ltr_wetlands/main_project/data/water_budget/Wetland Sensor Check Log.csv"

pvc.hts.fp <- "R:/ltr_wetlands/main_project/data/water_budget/Wetland_PVC_Heights.csv"


# flpth <- str_c(hobo.data.fp,"/202309_Sep-2023/FP04_H2Olvl_20230920.csv")


### ALL EQUATIONS BASED ON ONSET DOCUMENTATION ACCESSED HERE:
# https://www.onsetcomp.com/resources/tech-notes/barometric-compensation-method


# FUNCTION TO IMPORT AND COMBINE RAW DATA --------------------

# create a function that reads in lai files (.txt) consistently and adds original filename to each record.
 
read.files <- function(flpth){
  
  # pulls the first row in each file containing column names and creates r-friendly names for further processing of metadata (sensor num, units) 
  header1 <- names(read_csv(flpth, skip = 1, n_max = 0)) %>%
    make_clean_names(use_make_names = F, 
                     parsing_option = 0,
                     replace = c(`\\(` = "_", `\\)` = "_", `#` = "num", `°` = "_"))
  header1
  
  # extract the units of temp (can vary, depending on how logger was launched)  
  # if(sum(str_detect(header1, "temp")) > 0) #ensures temp is there before querying
     temp_units <- header1 %>%
         str_extract(., "(?<=temp\\.).") %>% 
         .[!is.na(.)]
  
  
  temp_units
  
  #extract units of pressure (varies depending on how logger was launched)
  # if(sum(str_detect(header1, "abs.pres")) > 0)
  pres_units <- header1 %>%
         str_extract(., "(?<=abs.pres,)...") %>% 
         .[!is.na(.)]
  
  
  pres_units
  
  #extract gmt offset (times were not always set to standard)
  # if(sum(str_detect(header1, "gmt-")) > 0)
    gmt_offset_hrs <- header1 %>% 
      str_extract(., "(?<=gmt-)..") %>% 
      .[!is.na(.)]
  
  
  gmt_offset_hrs
  
  # extract 8-digit sensor serial number
  # if(sum(str_detect(header1, "end.of_file_lgr_s/n:")) > 0)
  sensor_id <- header1 %>% 
    str_extract(., "(?<=end.of_file_lgr_s/n:)........") %>% 
    .[!is.na(.)]
  
  sensor_id
  
  # extract site name (W69, OS78, or DS2; all these wetlands have air press gauges installed)
  
  plot_title <- names(read_csv(flpth, n_max = 1)) %>%
    str_replace(., "Plot Title: ", "")
 
  site_id <- {
    if(str_detect(plot_title , pattern = "H2Olvl|H20lvl|ppt")) {
      str_extract(plot_title, pattern = ".*(?=_H2Olvl)|.*(?=_H20lvl)|.*(?=_ppt)")} #should get all wetland/flowpath IDs except for a few loggers which weren't labeled with the same conventions, below
    else if(str_detect(plot_title , pattern = "king|King")) {"W11"}
    else if(str_detect(plot_title , pattern = "Predest")) {"W58"}
    else if(str_detect(plot_title , pattern = "air")) {"W69"}
    else if(str_detect(plot_title , pattern = "OS78")) {"OS78"}
    else if(str_detect(plot_title , pattern = "DS2")) {"W58"}
    else {NA_character_}
  }
  
  site_id
  
  
  # clean up the header now that we've gotten all the metadata out that we need. 
  header2 <- header1 %>%
    str_replace_all(., "lgr_s/n:........,sen_s/n:........", "") %>% #remove appended logger/sensor serial numbers
    str_replace(., ",gmt-.....", "") %>% #remove appended gmt time offset
    str_replace_all(., ",psi_|,kpa_|.c_|.f_", "") %>% # appended remove units
    str_replace_all(., "_$|\\.$", "") %>%  #remove underscores and periods at the ends of col names
    str_replace_all(., "_", ".") #replace last _ with "." so all names are consistent
  
  header2
  
  
  read_csv(flpth, skip = 2, col_names = header2, col_types = cols(.default = "c")) %>%  # script for reading a file into R and adding filename and datalogger program as metadata columns
    select(-num) %>%
    mutate(gmt.offset= if_else(!is.null(gmt_offset_hrs), gmt_offset_hrs, NA_character_),
           temp.units = if_else(!is.null(temp_units), temp_units, NA_character_),
           pres.units = if_else(!is.null(pres_units), pres_units, NA_character_), 
           sensor.id = if_else(!is.null(sensor_id), sensor_id, NA_character_),
           site.id = if_else(!is.null(site_id), site_id, NA_character_),
           filename = flpth #basename(flpth)
    ) %>% 
    select(site.id, date.time.orig = date.time, gmt.offset, abs.pres, pres.units, temp, temp.units, sensor.id, filename, everything())
  

}
 


# define specific pattern with which all files we're interested in can be located
H2Olvl.filename.pattern <- "_H2Olvl_.*\\.csv$|_H20lvl_.*\\.csv$"#"FP04_H2Olvl_20230920\\.csv"#"_H2Olvl_.*\\.csv$|_H20lvl_.*\\.csv$"
    
# load all files in that match the above pattern, using the read.filed function.
#includes creating a column for the original timestammp on senor records (often offset) and the adjusted timestamp every 15 mins, so all sensors can be merged easily

h2olvl.compiled <-
  list.files(path = hobo.data.fp,
             recursive = TRUE,
             full.names = TRUE,
             pattern = H2Olvl.filename.pattern) %>%  
  str_subset(., "CORRECTED DATA|temp from drain wetlands|merged data|Files_To_Review", negate = TRUE) %>% # exclude files from the "CORRECTED DATA" folder & other folders with non-raw data files
  map_df(~read.files(.)) %>% 
  mutate(date.time.orig = mdy_hms(date.time.orig),
         abs.pres = as.numeric(abs.pres),
         temp = as.numeric(temp),
         abs.pres_kpa = case_when(pres.units == "kpa" ~ abs.pres, # get all pressure readings on the same unit, kpa.
                                  pres.units == "psi" ~ abs.pres * 6.89476,
                                  TRUE ~ NA_real_),
         temp_c = case_when(temp.units == "c" ~ temp, # get all temp readings to the same unit, Celsius
                            temp.units == "f" ~ (5/9)*(temp-32),
                            TRUE ~ NA_real_),
         date.time.est = case_when(gmt.offset == "05" ~ date.time.orig, # get all time stamps to the correct time (EST, not DST)
                                   gmt.offset == "04" ~ date.time.orig + (60*60),
                                   gmt.offset == "06" ~ date.time.orig - (60*60))
  ) %>% 
  select(site.id:sensor.id, abs.pres_kpa, temp_c, everything()) %>% 
  arrange(date.time.est) %>%
  distinct(., pick(site.id:temp_c), .keep_all = T) %>%
  mutate(date.time = round_date(date.time.est, unit = "15 mins")) %>% 
  select(-c(abs.pres, pres.units, temp, temp.units, date.time.orig, gmt.offset)) %>% 
  select(site.id, date.time, abs.pres_kpa, temp_c, sensor.id, filename, everything())

  
# separate the large file into two data sets: actual h2o lvl data and coupler event metadata

# h2o lvl data: just remove coupler event columns & lines with no h2o lvl data
h2olvl <- h2olvl.compiled %>% 
  select(site.id, date.time, abs.pres_kpa, temp_c, sensor.id, filename, date.time.est) %>% 
  filter(!is.na(abs.pres_kpa) & !is.na(temp_c))

### check for multiple records assigned to the same time stamp

dups <- h2olvl %>% 
  add_count(site.id, date.time) %>% 
  filter(n>1)

### A review of duplicates looks like *MOST* of the records are a result of the last record prior to a download and the first record after a relaunch
### were relatively similar and assigned to the same nearest 15 min. timestamp
### Other examples include files where timespans overlap (which can happen when downloaded direct to laptop & sensor isn't relaunched) but data are different
### In these cases, the data appear to be corrupted (i.e. obviously incorrect temps of -25 or 95 C) though all other records in the overlapping files match.
### Generally, it should be fine to group by site.id, arrange by date.time, and use distinct() to filter out overlapping records.

h2olvl2 <- h2olvl %>% 
  arrange(date.time.est) %>% 
  distinct(., pick(site.id, date.time), .keep_all=T)

#check dups again: should be 0
dups <- h2olvl2 %>% 
  add_count(site.id, date.time) %>% 
  filter(n>1)



# coupler events are MESSY and often logged at slightly different times than the actual data record.
# Below, we match all coupler events with the closest data record in time that occured after the coupler event,
# and also consolidate multiple coupler events of the same type that were recorded within a ~15 minute time span.

h2olvl_coupler_evnts <- h2olvl.compiled %>% 
  select(-c(date.time.est, abs.pres_kpa, temp_c, sensor.id, filename)) %>% #select only the coupler metadata, excluding messy time stamps
  filter(!is.na(coupler.detached) | 
           !is.na(coupler.attached) | 
           # !is.na(host.connected) | 
           !is.na(end.file) | 
           !is.na(stopped) | 
           !is.na(batt.v) #| 
           # !is.na(bad.battery) | 
           # !is.na(good.battery)
         ) %>% #filter out records with NO coupler data recorded
  # pivot_longer(., cols = c(coupler.detached:good.battery), names_to = "cplr_evnt", values_to = "logged") %>% # pivot longer to reduce duplication of information
  pivot_longer(., cols = c(coupler.detached:batt.v), names_to = "cplr_evnt", values_to = "logged") %>% # pivot longer to reduce duplication of information
  filter(!is.na(logged)) %>% #keep only logged events
  distinct() %>% # remove dups
  pivot_wider(names_from = "cplr_evnt", values_from = "logged") #return to wide format
 
# Finally, rejoin coupler events to water level data
h2olvl3 <- h2olvl2 %>% 
  left_join(h2olvl_coupler_evnts) %>%
  ungroup() %>% 
  #pad the time stamps for each site's records
  pad(., by = "date.time", group = "site.id")

### SO far this works with smaller dataset
 

ggplot(h2olvl2, aes(x = date.time, y = abs.pres_kpa, color= site.id)) + 
  geom_point() +
  facet_wrap(~site.id, scales = "free")  
  

ggplot(h2olvl2, aes(x = date.time, y = temp_c)) + 
  geom_point() +
  facet_wrap(~ site.id, scales = "free")

# Remove outliers - obvious erroneous data
# Remove datapoints immediately before or after data collection (where a coupler event was logged)

# first create a custom function for calculating tukey's extreme outlier fences

out.hi_fun <- function(x, na.rm = TRUE) {
  q1 <- quantile(x, probs = 0.25, na.rm = na.rm)
  q3 <- quantile(x, probs = 0.75, na.rm = na.rm)
  iqr <- q3-q1
  hi <- q3 + 3*iqr #hi extreme out
  
  hi
}

out.lo_fun <- function(x, na.rm = TRUE) {
  q1 <- quantile(x, probs = 0.25, na.rm = na.rm)
  q3 <- quantile(x, probs = 0.75, na.rm = na.rm)
  iqr <- q3-q1
  lo <- q1 - 3*iqr #low extreme out
  
  lo
}

cv_fun <- function(x, na.rm = TRUE) {
  m <-  mean(x, na.rm = na.rm)
  sd <- sd(x, na.rm = na.rm)
  cv <- sd/m
  }

# calculate rolling outlier fences, and filter out all outliers and records collected immediately before or after a data download.
h2olvl4 <- h2olvl3 %>% 
  # filter(site.id %in% c("FA_BWE", "FP02", "FP04", "W68", "W41", "W53")) %>% 
  group_by(site.id) %>% 
  mutate(#daily_avg = roll_meanr(abs.pres.kpa, n=96, na.rm = F),
         #daily_sd = roll_sdr(abs.pres.kpa, n=96, na.rm = F),
         dp = abs.pres_kpa - lag(abs.pres_kpa),
         dp.m = roll_mean(dp, 96*2, fill = NA, align = "center", na.rm = F),
         dp.sd = roll_sd(dp, 96*2, fill = NA, align = "center", na.rm = F),
         # dp.cv = dp.sd/dp.m,
         dt = temp_c - lag(temp_c),
         dt.m = roll_mean(dt, 96*2, fill = NA, align = "center", na.rm = F),
         dt.sd = roll_sd(dt, 96*2, fill = NA, align = "center", na.rm = F),
         # dt.cv = dt.sd/dt.m,
         # cv.dt = rollapply(dt, width = 96, cv_fun, fill = NA, align = "center")
         # dp.hi = rollapply(dp, width=96, out.hi_fun, fill = NA, align = "center"),
         # dp.lo = rollapply(dp, width=96, out.lo_fun, fill = NA, align = "center"),
         # dt.hi = rollapply(dt, width=96, out.hi_fun, fill = NA, align = "center"),
         # dt.lo = rollapply(dt, width=96, out.lo_fun, fill = NA, align = "center"),
         pres.m = roll_mean(abs.pres_kpa, 96*2, fill = NA, align = "center", na.rm = F),
         pres.sd = roll_sd(abs.pres_kpa, 96*2, fill = NA, align = "center", na.rm = F),
         temp.m = roll_mean(temp_c, 96*2, fill = NA, align = "center", na.rm = F),
         temp.sd = roll_sd(temp_c, 96*2, fill = NA, align = "center", na.rm = F),
         # pres.hi = rollapply(abs.pres_kpa, width=96, out.hi_fun, fill = NA, align = "center"),
         # pres.lo = rollapply(abs.pres_kpa, width=96, out.lo_fun, fill = NA, align = "center"),
         # temp.hi = rollapply(temp_c, width=96, out.hi_fun, fill = NA, align = "center"),
         # temp.lo = rollapply(temp_c, width=96, out.lo_fun, fill = NA, align = "center"),
         coupler_evnt = if_else(is.na(coupler.detached) & is.na(coupler.attached) #& is.na(host.connected) 
                                # & is.na(end.of.file) 
                                & is.na(stopped), 0, 1),
         data_filter = case_when(coupler_evnt == 1 | lead(coupler_evnt) == 1 | lag(coupler_evnt) == 1 | lag(coupler_evnt, 2) == 1 ~ "coupler event",
                            ((dp > dp.m + dp.sd*3 | dp < dp.m - dp.sd*3) 
                            & (abs.pres_kpa > pres.m + pres.sd*3 | abs.pres_kpa < pres.m - pres.sd*3)) ~ "pressure outlier",     
                            (((dt > .2 & dt > dt.m + dt.sd*3) | (dt < -.2 & dt < dt.m - dt.sd*3)) 
                             & (temp_c > temp.m + temp.sd*3 | temp_c < temp.m - temp.sd*3)) ~ "temp outlier",  
                            # dt > dt.m + dt.sd*3 | dt < dt.m - dt.sd*3 ~ "temp stability outlier",
                            # dt > 3 | dt < - 1 ~ "temp change outlier",
                            # abs.pres_kpa > pres.hi | abs.pres_kpa < pres.lo ~ "pressure outlier",
                            # temp_c > temp.hi | temp_c < temp.lo ~ "temp outlier",
                            TRUE ~ NA_character_)

  )


h2olvl4

h2olvl_fltrd %>%
  filter(site.id == "W69") %>%
  # filter(date.time > ymd("2021-04-23") & date.time < ymd("2021-05-07")) %>%
  ggplot(aes(x = date.time, y = abs.pres_kpa)) + 
  geom_point() #+
  geom_point(data = filter(h2olvl4, !is.na(data_filter) 
                           # & date.time > ymd("2021-04-23") & date.time < ymd("2021-05-07")
                           & site.id == "W69"
                           ), aes(y = abs.pres_kpa, color = data_filter)) +
  # geom_ribbon(aes(ymin = pres.lo, ymax = pres.hi), alpha = 0.1)+
  facet_wrap(~ site.id, scales = "free")
             
h2olvl4 %>%
  filter(site.id == "W02") %>%
  # filter(date.time > ymd("2021-04-23") & date.time < ymd("2021-05-07")) %>%  
  ggplot(aes(x = date.time, y = temp_c)) + 
  geom_point() +
  geom_point(data = filter(h2olvl4, !is.na(data_filter) 
                           # & date.time > ymd("2021-04-23") & date.time < ymd("2021-05-07")
                           & site.id == "W02"
  ), aes(y = temp_c, color = data_filter)) +
  # geom_ribbon(aes(ymin = pres.lo, ymax = pres.hi), alpha = 0.1)+
  facet_wrap(~ site.id, scales = "free")


### SO FAR THIS FILTERING METHOD SEEMS TO WORK OKAY

h2olvl_fltrd <- h2olvl4 %>% 
  mutate(temp_c = if_else(is.na(data_filter), temp_c, NA_real_),
         abs.pres_kpa = if_else(is.na(data_filter), abs.pres_kpa, NA_real_))


##### IMPORT MANUAL TAPE DOWN RECORDS AND PVC HEIGHT DATA

unique(h2olvl_fltrd$site.id)

td <- read_csv(manual.levels.fp) %>% 
  clean_names() %>% 
  filter(!is.na(tape_dwn_rcrd) | h2olvl_status == "dry")

# 1063 events total as of 11/6/2023

td

pvc.hts <- read_csv(pvc.hts.fp) %>% 
  clean_names()

pvc.hts

td$wetland %>% unique()

td <- td %>% 
  filter(wetland != "W69_Air") %>% 
  left_join(., select(pvc.hts, wetland = site_id, pvc_ht_ft)) %>% 
  mutate(
    date.time = ymd_hms(str_c(as.character(date), as.character(time))), 
    site.id = case_when(
      str_detect(wetland, "BP.") ~ str_c("BP0", str_sub(wetland, -1)),
      TRUE ~ wetland),
    td_dec.ft = case_when(h2olvl_status == "dry" ~ pvc_ht_ft,
                          tape_dwn_units == "ft" ~ tape_dwn_rcrd,
                          tape_dwn_units == "in" ~ tape_dwn_rcrd/12),
    h2olvl.man_ft = pvc_ht_ft- td_dec.ft,
    h2olvl.man_m = 0.3048*h2olvl.man_ft
  ) 
  
#Review data
td %>% 
  filter(is.na(wetland)
         | is.na(date.time)
         | date.time > today() 
         | date.time < as_date("2016-12-09")) %>% 
  print(., n = 60)
# some fail to parse b/c time data not currently available for that record. 
# Specifically, the 10/25-10/26 2017 wetland trip needs times.
# Also 2017/06/19 FP04
# Also 2019 04/15 - 04/17
# Also W02, W03, W11, and W52 no date or time

# three dates out of range
# W53 - 2008/7/25; W03 - 1900/01/09; W42 2002-11-01 

# In total: 47 records that won't match actual data

td %>% 
  filter(h2olvl.man_m < 0
         | h2olvl.man_m > 4) %>% 
  select(date.time, site.id, td_dec.ft, pvc_ht_ft, h2olvl.man_m) %>% 
  print(n = 40)

# also review W69 2018/06/18. Tapedown is way off. Wrong units or missing decimal point.

### Create a csv of data to review
td.errors <- td %>% 
  filter(is.na(wetland)
         | is.na(date.time)
         | date.time > today() 
         | date.time < as_date("2016-12-09")
         | h2olvl.man_m < - 0.1
         | h2olvl.man_m > 4) %>% 
  print(., n = 60)

write_csv(td.errors, "R:/ecohydrology/staff/Stribling/projects/wetlands and wells/td.errors.csv")

td %>% arrange(desc(date.time))

ggplot(filter(td, h2olvl.man_ft > -1, 
              (date.time < today() & date.time > as_date("2016-12-09"))), 
              aes(x = date.time, y = h2olvl.man_ft)) +
  geom_point()+
  facet_wrap(~site.id)

## filter & clean td records
# remove weird dates (for now)
# round all dates to nearest 15 min to join with wetland data
# change all negative tds to 0

td <- td %>% 
  mutate(h2olvl.man_ft = if_else(h2olvl.man_ft < 0, 0, h2olvl.man_ft),
         h2olvl.man_m = 0.3048*h2olvl.man_ft,
         date.time = round_date(date.time, "15 minutes")) %>% 
  filter(date.time < today() & date.time > as_date("2016-12-09")) %>%
  filter()

td

# Processing Water Level Data using Barometric Pressure Data 

### NEED TO BRING IN BULK PRESSURE DATA AND REF LEVEL READINGS

# When temp data is available, it's derived from the temperature using the following equation (https://www.onsetcomp.com/resources/tech-notes/barometric-compensation-method)

# p(density.. units?) =  (999.83952 + (16.945176*temp.c) - .0079870401*(temp.c^2) - .000046170461*(temp.c^3) + .00000010556302*(temp.c^4) - .00000000028054253*(temp.c^5))/(1+ .016879850*temp.c)

# If temp was not recorded, assume 1,000 kg/m^3 (fresh water)
  
# ## td values are not joining properly. 
# ## anti join to see which rows are not joining to main table 
# 
# test <- h2olvl_fltrd %>%
#   anti_join(select(td, site.id, date.time, h2olvl_m, h2olvl_ft), .)
# actually they are joining properly; we only have one month;s worth of data in our test data set and 18 records joined; one for each wetland.

h2olvl5 <- h2olvl_fltrd %>%
  select(site.id, date.time, abs.pres_kpa, temp_c, sensor.id, filename) %>% 
  left_join(., kpa.w69) %>% 
  left_join(., select(td, site.id, date.time, h2olvl.man_m, h2olvl.man_ft)) %>% 
  arrange(site.id, date.time) %>% 
  mutate(#abs.pres_psi = abs.pres_kpa * 0.1450377,
         #baro.pres_psi = baro.pres_kpa * 0.1450377,
         fl.density_kg.m3 = (999.83952 + (16.945176*temp_c) - .0079870401*(temp_c^2) - .000046170461*(temp_c^3) + .00000010556302*(temp_c^4) - .00000000028054253*(temp_c^5))/(1 + .016879850*temp_c),
         fl.density_lb.ft3 = fl.density_kg.m3 * 0.0624279606,
         depth.h2o_m = 0.3048*(0.1450377*144.0*abs.pres_kpa)/fl.density_lb.ft3,
         # depth.h2o_ft = (0.1450377*144.0*abs.pres_kpa)/fl.density_lb.ft3,
         depth.baro_m = 0.3048*(0.1450377*144.0*baro.pres_kpa)/fl.density_lb.ft3,
         # depth.baro_ft = (0.1450377*144.0*baro.pres_kpa)/fl.density_lb.ft3,
         comp.const.k_m = if_else(!is.na(h2olvl.man_m), h2olvl.man_m - (depth.h2o_m - depth.baro_m), NA_real_)) %>%  
         # comp.const.k_ft = if_else(!is.na(h2olvl.man_ft), h2olvl.man_ft - (depth.h2o_ft - depth.baro_ft), NA_real_)) %>%
  group_by(site.id) %>% 
  arrange(date.time) %>%
  filter(site.id %in% (.$site.id[!is.na(.$comp.const.k_m)] %>% unique())) %>%  #filters out data from sites without any k value
  mutate(comp.const.k_m2 = na.approx(comp.const.k_m, rule = 2),### INTERPOLATE k between manual measurements
         # comp.const.k_ft2 = na.approx(comp.const.k_ft, rule = 2),
         h2olvl.real_m = depth.h2o_m - depth.baro_m + comp.const.k_m2,
         h2olvl.real_ft = 3.28084*depth.h2o_m)
         # h2olvl.real_m_con = 0.3048*h2olvl.real_ft)#,
         # h2olvl.real_m2 = 0.3048*h2olvl.real_m2)

filter(h2olvl5, !is.na(h2olvl.man_ft)) %>% 
  select(site.id, date.time, h2olvl.man_ft, comp.const.k, comp.const.k2)

h2olvl_fltrd$site.id %>% unique()
#These numbers look right^ density is right at ~ 999 kg/m^3
## Will need to gapfill, correlating water temp with air temp. (? <- what does this mean)

####PLOT

# h2olvl5 %>% 
#   ggplot(aes(date.time, h2olvl.real_m)) +
#   geom_point() +
#   facet_wrap(~ site.id, scales = "free")


#### BRING IN TRIAL DATA PROCESSED VIA HOBO
## SEE "read_processed_hobo_files.R" for script that brings in a few datasets that were processed using Hoboware
h2olvl5 %>% 
  ggplot(aes(date.time, h2olvl.real_m)) +
  geom_point() +
  # geom_point(aes(y = h2olvl.real_m), alpha = 0.3, color = "blue3") +
  geom_point(data = h2olvl.processed.compiled, aes(x = date.time, y = water.level_m), alpha = 0.3, color = "green4") +
  facet_wrap(~ site.id, scales = "free")


h2olvl.diff <- h2olvl5 %>% 
  select(date.time, h2olvl.real_m, abs.pres_kpa, baro.pres_kpa, depth.baro_m, depth.h2o_m) %>% 
  left_join(select(h2olvl.processed.compiled, site.id, date.time, water.level_m)) %>% 
  mutate(level.diff = h2olvl.real_m - water.level_m) %>% 
  filter(!is.na(water.level_m)) %>% 
  arrange(site.id, date.time)

