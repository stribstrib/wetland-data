#TEST SCRIPT TO IMPORT AND ANALYZE WETLAND DATA

.libPaths("C:/Users/stribling.stuber/documents/R/win-library/4.2")


library(tidyverse)
library(janitor)
library(padr)
library(RcppRoll)
library(zoo)


# list filepath to main dataset
hobo.data.fp <- "R:/ltr_wetlands/main_project/data/water_budget/hoboFiles"

flpth <- str_c(hobo.data.fp,"/201707_July-2017/W21_ppt_20170724.csv")



# FUNCTION TO IMPORT AND COMBINE RAW DATA --------------------

# create a function that reads in ppt files (.csv) consistently and adds original filename to each record.

read.files <- function(flpth){
  
  # pulls the first row in each file containing column names and creates r-friendly names for further processing of metadata (sensor num, units) 
  header1 <- names(read_csv(flpth, skip = 1, n_max = 0)) %>%
    make_clean_names(use_make_names = F, 
                     parsing_option = 0,
                     replace = c(`\\(` = "_", `\\)` = "_", `#` = "num", `°` = "_"))
  header1
  
  # extract the units of temp (can vary, depending on how logger was launched)  
  temp_units <- header1 %>%
    str_extract(., "(?<=temp\\.).") %>% 
    .[!is.na(.)]
  
  temp_units
  
  #extract units of pressure (varies depending on how logger was launched)
  precip_units <- header1 %>%
    str_extract(., "(?<=precip\\.)..") %>% 
    .[!is.na(.)]
  
  precip_units
  
  #extract gmt offset (times were not always set to standard)
  gmt_offset_hrs <- header1 %>% 
    str_extract(., "(?<=gmt-)..") %>% 
    .[!is.na(.)]
  
  gmt_offset_hrs
  
  # extract 8-digit sensor serial number
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
    mutate(gmt.offset = gmt_offset_hrs,
           temp.units = temp_units,
           pres.units = pres_units,
           sensor.id = sensor_id,
           site.id = site_id,
           filename = flpth #basename(flpth)
    ) %>% 
    select(site.id, date.time.orig = date.time, gmt.offset, abs.pres, pres.units, temp, temp.units, sensor.id, filename, everything())
  
  #   # pulls the first row in each file containing column names, drops sensor-specific numbers, and creates r-friendly names 
  #   header <- names(read_csv(flpth, n_max = 0)) %>% 
  #     make_clean_names(use_make_names = FALSE, parsing_option = 0) %>%
  #     str_remove_all(., "[:digit:]") %>%
  #     str_remove(., "_number_|\\)number_|.number_") %>% 
  #     str_replace_all(., "_|\\(", ".")
  #   
  #   sensor_num <- names(read_csv(flpth, n_max = 0)) %>%  # pulls the first row in each file containing sensor id metadata
  #     str_extract(., "(\\d)+$") #extracts only the number (sensor id) from teh string ("$" makes it ONLY numbers at the end of strings)
  #   
  #   site_name <-  basename(flpth) %>% 
  #     str_extract(., "^.*(?=_H2Olvl)|^.*(?=_ppt)")
  #   
  # #   # extract the site name
  # #   if(str_detect(basename(flpth), "King|king"){"W11"}
  # #      if(str_detect(basename(flpth), "Predest|predest")) {"W58"
  # #        if(str_detect(basename(flpth), "air")){"W69_air"
  # #          if(str_detect(basename(flpth), "_H2Olvl|_H20lvl|_ppt"){str_extract(basename(flpth), "^.*(?=_H2Olvl)|^.*(?=_H20lvl)|^.*(?=_ppt)")
  # #          }
  # #        }
  # #      }
  # # } else {NA_character_}
  #   
  #   
  #   read_tsv(flpth, skip = 1, col_names = header) %>%  # script for reading a file into R and adding filename and datalogger program as metadata columns
  #     mutate(sensor_id = sensor_num,
  #            site_id = site_name,
  #            filename = basename(flpth))
}

# read.files <- function(flnm){
#   read_tsv(flnm) %>%  # script for reading tab separated value files into R, ensuring consistent varaible types, and adding filename as metadata column
#     clean_names() %>% 
#     mutate(filename = flnm)
# }
# 

# define specific pattern with which all files we're interested in can be located
H2Olvl.filename.pattern <- "_H2Olvl_.*\\.csv$|_H20lvl_.*\\.csv$" 

# load all files in that match the above pattern, using the read.filed function.
#includes creating a column for the original timestammp on senor records (often offset) and the adjusted timestamp every 15 mins, so all sensors can be merged easily
h2olvl.compiled <-              
  list.files(path = hobo.data.fp,
             recursive = TRUE,
             full.names = TRUE,
             pattern = H2Olvl.filename.pattern) %>%  
  str_subset(., "CORRECTED DATA", negate = TRUE) %>% # exclude files from the "CORRECTED DATA" folder; I believe these have been processed?
  map_df(~read.files(.)) %>%
  mutate(date.time.orig = mdy_hms(date.time.orig),
         abs.pres = as.numeric(abs.pres),
         temp = as.numeric(temp),
         abs.pres_kpa = case_when(pres.units == "kpa" ~ abs.pres, # get all pressure readings on the same unit, kpa.
                                  pres.units == "psi" ~ abs.pres * 6.89476,
                                  TRUE ~ NA_real_),
         temp_c = case_when(temp.units == "c" ~ temp, # get all temp readings to the same unit, celcius
                            temp.units == "f" ~ (5/9)*(temp-32),
                            TRUE ~ NA_real_),
         date.time.est = case_when(gmt.offset == "05" ~ date.time.orig, # get all time stamps to the correct time (EST, not DST)
                                   gmt.offset == "04" ~ date.time.orig - (60*60))
  ) %>% 
  arrange(date.time.est) %>% 
  distinct(., pick(site.id:sensor.id), .keep_all = T) %>% 
  mutate(date.time = round_date(date.time.est, unit = "15 mins")) %>% 
  select(-c(abs.pres, pres.units, temp, temp.units, date.time.orig, gmt.offset)) %>% 
  select(site.id, date.time, abs.pres_kpa, temp_c, sensor.id, filename, everything())

# list.files(path = c(hobo.data.fp),
#            full.names = TRUE,
#            pattern = glob2rx(H2Olvl.filename.pattern),
#            recursive = T)  %>% 
# map_df(~read.files(.)) %>% 
# mutate(date.time.orig = mdy_hms(date.time)) %>% 
# arrange(date.time.orig) %>% 
# select(site_id, date.time.orig, abs.pres.kpa:end.of.file, stopped, sensor_id, filename) %>% 
# distinct(., pick(site_id:sensor_id), .keep_all = T) %>% 
# mutate(date.time = round_date(date.time.orig, unit = "15 mins"))

# separate the large file into two data sets: actual h2o lvl data and coupler event metadata

# h2o lvl data: just remove coupler event columns & lines with no h2o lvl data
h2olvl <- h2olvl.compiled %>% 
  select(date.time, site_id, abs.pres.kpa, temp.c, sensor_id, filename, date.time.orig) %>% 
  filter(!is.na(abs.pres.kpa) & !is.na(temp.c))


# coupler events are MESSY and often logged at slightly different times than the actual data record.
# Below, we match all coupler events with the closest data record in time that occured after the coupler event,
# and also consolidate multiple coupler events of the same type that were recorded within a ~15 minute time span.
h2olvl_coupler_evnts <- h2olvl.compiled %>% 
  select(-c(date.time.orig, abs.pres.kpa, temp.c, sensor_id, filename)) %>% #select only the coupler metadata, excluding messy time stamps
  filter(!is.na(coupler.detached) | !is.na(coupler.detached) | !is.na(coupler.attached) | !is.na(host.connected) | !is.na(end.of.file) | !is.na(stopped)) %>% #filter out records with NO coupler data recorded
  # difference_left_join(., select(h2olvl, site_id, date.time), by = c("date.time" = "date.time"), max_dist = 15*60, distance_col = "dist") %>% 
  # filter(site_id.x == site_id.y) %>% 
  # group_by(across(site_id.x:stopped)) %>% # group by all records from coupler events table
  # slice_min(dist) %>% # and then reduce the list to include ONLY the data records which match the closest to the coupler records 
  # ungroup() %>% 
  # mutate(join.flag = if_else(dist > 15*60, "no matching logged data", NA_character_), #flag coupler events that don't have associated datapoints in time 
  #        date.time = if_else(is.na(join.flag), date.time.y, date.time.x)) %>%  # assign the time of the data records to the coupler records, except where there is no match.
  # select(-c(site_id.x, date.time.x, date.time.y,dist)) %>% 
  # select(site_id = site_id.y, date.time, everything()) %>%  # arrange data & remove columns
  # # group_by(site_id, date.time, join.flag) %>% 
  pivot_longer(., cols = c(coupler.detached:stopped), names_to = "cplr_evnt", values_to = "logged") %>% # pivot longer to reduce duplication of information
  filter(logged == "Logged") %>% #keep only logged events
  distinct() %>% # remove dups
  pivot_wider(names_from = "cplr_evnt", values_from = "logged") #return to wide format


# Finally, rejoin coupler events to water level data
h2olvl2 <- h2olvl %>% 
  left_join(h2olvl_coupler_evnts) %>%
  pad(by = "date.time")

ggplot(h2olvl2, aes(x = date.time, y = abs.pres.kpa)) + 
  geom_point()

ggplot(h2olvl2, aes(x = date.time, y = temp.c)) + 
  geom_point()

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


# calculate rolling outlier fences, and filter out all outliers and records collected immediately before or after a data download.
h2olvl3 <- h2olvl2 %>% 
  mutate(#daily_avg = roll_meanr(abs.pres.kpa, n=96, na.rm = F),
    #daily_sd = roll_sdr(abs.pres.kpa, n=96, na.rm = F),
    pres.hi = rollapply(abs.pres.kpa, width=96*2, out.hi_fun, fill = NA, align = "center"),
    pres.lo = rollapply(abs.pres.kpa, width=96*2, out.lo_fun, fill = NA, align = "center"),
    temp.hi = rollapply(temp.c, width=96*2, out.hi_fun, fill = NA, align = "center"),
    temp.lo = rollapply(temp.c, width=96*2, out.lo_fun, fill = NA, align = "center"),
    coupler_evnt = if_else(is.na(coupler.detached) & is.na(coupler.attached) & is.na(host.connected) & is.na(end.of.file) & is.na(stopped), 0, 1),
    data_filter = case_when(coupler_evnt == 1 | lead(coupler_evnt) == 1 | lag(coupler_evnt) == 1 | lag(coupler_evnt, 2) == 1 ~ "coupler event",
                            abs.pres.kpa > pres.hi | abs.pres.kpa < pres.lo ~ "pressure outlier",
                            temp.c > temp.hi | temp.c < temp.lo ~ "temp outlier",
                            TRUE ~ NA_character_)
    
  )


h2olvl3

h2olvl3 %>%
  # filter(date.time > ymd("2017-02-01") & date.time < ymd("2017-02-28")) %>%  
  ggplot(aes(x = date.time, y = abs.pres.kpa)) + 
  geom_point() +
  geom_point(data = filter(h2olvl3, !is.na(data_filter)), aes(x = date.time, y = abs.pres.kpa, color = data_filter)) +
  geom_ribbon(aes(ymin = pres.lo, ymax = pres.hi), alpha = 0.1)

h2olvl3 %>% 
  # filter(date.time > ymd("2017-02-01") & date.time < ymd("2017-02-28")) %>%  
  ggplot(aes(x = date.time, y = temp.c)) + 
  geom_point()+
  geom_point(data = filter(h2olvl3, !is.na(data_filter)), aes(x = date.time, y = temp.c, color = data_filter)) +
  geom_ribbon(aes(ymin = temp.lo, ymax = temp.hi), alpha = 0.1)


### SO FAR THIS FILTERING METHOD SEEMS TO WORK WELL

h2olvl_fltrd <- h2olvl3 %>% 
  filter(is.na(data_filter))



# Processing Water Level Data using Barometric Pressure Data 

### NEED TO BRING IN BULK PRESSURE DATA AND REF LEVEL READINGS

# When temp data is available, it's derived from the temperature using the following equation (https://www.onsetcomp.com/resources/tech-notes/barometric-compensation-method)

# p(density.. units?) =  (999.83952 + (16.945176*temp.c) - .0079870401*(temp.c^2) - .000046170461*(temp.c^3) + .00000010556302*(temp.c^4) - .00000000028054253*(temp.c^5))/(1+ .016879850*temp.c)

# If temp was not recorded, assume 1,000 kg/m^3 (fresh water)

h2olvl4 <- h2olvl_fltrd %>%
  mutate(fl.density_kg.m3 = (999.83952 + (16.945176*temp.c) - .0079870401*(temp.c^2) - .000046170461*(temp.c^3) + .00000010556302*(temp.c^4) - .00000000028054253*(temp.c^5))/(1+ .016879850*temp.c),
         fl.density_lb.ft3 = fl.density_kg.m3 * 0.0624279606,
         depth.h2o_ft = 0.3048*(0.1450377*144.0*abs.pres.kpa)/fl.density_lb.ft3,
         depth.baro_ft = 0.3048*(0.1450377*144.0*BAROMETRIC REF KPA)/fl.density_lb.ft3,
         comp.const.k = MANUAL_DEPTH - (depth.h2o_ft - depth.baro_ft),
         lreal = depth.h2o_ft - depth.baro_ft + comp.const.k)

#These numbers look right^ density is right at ~ 999 kg/m^3
## Will need to gapfill, correlating water temp with air temp.


