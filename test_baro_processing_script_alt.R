#TEST SCRIPT TO IMPORT BAROMETRIC DATA

.libPaths("C:/Users/stribling.stuber/documents/R/win-library/4.2")

# library(IRanges)
library(tidyverse)
library(janitor)
library(padr)
# library(RcppRoll)
# library(zoo)
# library(fuzzyjoin)


# list filepath to main dataset
hobo.data.fp <- "R:/ltr_wetlands/main_project/data/water_budget/hoboFiles"


flpth <- str_c(hobo.data.fp,"/201707_July-2017/air_20170726.csv")

# flpth

# FUNCTION TO IMPORT AND COMBINE RAW DATA --------------------

# create a function that reads in lai files (.txt) consistently and adds original filename to each record.

read.baro.files <- function(flpth){
  
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
  pres_units <- header1 %>%
    str_extract(., "(?<=abs.pres,)...") %>% 
    .[!is.na(.)]
  
  pres_units
  
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
    if(str_detect(plot_title , pattern = "air")) {"W69"}
    else if(str_detect(plot_title , pattern = "OS78")) {"OS78"}
    else if(str_detect(plot_title , pattern = "DS2")) {"DS2"}
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
}

# define specific pattern with which all files we're interested in can be located
air.filename.pattern <- "air.*\\.csv$|ATM.*\\.csv$|Atm.*\\.csv$" # filters files to include only .csv files with "air", "ATM" or "Atm" in the name

# load all files in that match the above pattern, using the read.filed function.
#includes creating a column for the original timestammp on senor records (often offset) and the adjusted timestamp every 15 mins, so all sensors can be merged easily
baro.compiled <-                                  
  list.files(path = c(hobo.data.fp),
             recursive = TRUE,
             full.names = TRUE,
             pattern = air.filename.pattern) %>%  
  str_subset(., "CORRECTED DATA", negate = TRUE) %>% # exclude files from the "CORRECTED DATA" folder; I believe these have been processed?
  map_df(~read.baro.files(.)) %>%
  mutate(date.time.orig = mdy_hms(date.time.orig),
         abs.pres = as.numeric(abs.pres),
         temp = as.numeric(temp),
         baro.pres_kpa = case_when(pres.units == "kpa" ~ abs.pres, # get all pressure readings on the same unit, kpa.
                                  pres.units == "psi" ~ abs.pres * 6.89476,
                                  TRUE ~ NA_real_),
         temp_c = case_when(temp.units == "c" ~ temp, # get all temp readings to the same unit, celcius
                            temp.units == "f" ~ (5/9)*(temp-32),
                            TRUE ~ NA_real_),
         date.time.est = case_when(gmt.offset == "05" ~ date.time.orig, # get all time stamps to the correct time (EST, not DST)
                                   gmt.offset == "04" ~ date.time.orig + (60*60),
                                   gmt.offset == "06" ~ date.time.orig - (60*60))
         ) %>% 
  arrange(date.time.est) # %>%

# # select(site_id, date.time.orig, abs.pres:end.of.file, stopped, sensor_id, filename) %>% 
# distinct(., pick(site.id:sensor.id), .keep_all = T) %>% 
#   # mutate(date.time = round_date(date.time.est, unit = "15 mins")) %>% 
#   select(-c(abs.pres, pres.units, temp, temp.units, date.time.orig, gmt.offset)) %>% 
#   select(site.id, date.time, abs.pres_kpa, temp_c, sensor.id, filename, everything())


### CHECK FOR ANY MAJOR DATE-TIME OVERLAPS.
### This may include actual duplicate records, where date time, site, and raw pressure and temp data are all the same.
### these type of exact duplicates are not a problem - the record will be assigned to the earliest file in which it was included.
### These duplicates may also include records WHERE DATE-TIME IS THE SAME BUT RAW DATA IS NOT.
### THESE OCCURENCES INDICATE A FILE THAT HAS INCORRECT TIME-STAMPS OR CORRUPTED DATA

dup.count <- baro.compiled %>% 
  group_by(date.time.est, site.id, baro.pres_kpa, temp_c) %>% # identifies FULLY DUPLICATED data records
  add_count(name = "n.dup.recs") %>% 
  group_by(date.time.est, site.id) %>% # identifies ALL records where DATE-TIME is duplicated (includes records where RAW DATA DIFFERS)
  add_count(name = "n.dup.dttm") %>%
  filter(n.dup.dttm > n.dup.recs) %>% # filters out all records except those which erroneously appear as duplicates 
  group_by(filename) %>% 
  add_count(name = "n.dups.per.file")

## get list of problem files

check.baro.files <- dup.count$filename %>% unique()
check.baro.files  

### Manually inspect files and datasheets to detrmine issue
### ALL problem files come from an overlap of two downloads: one from 2021-02-26 and one from 2021-04-14
### Date and time of last record from 2021-04-14 matches up with date and time on data sheet (minus 1 hr to acct for Daylight savings)
### Date and time of FIRST record of 4-14 dataset also matches with recorded download time in Feb 2021.
### Date and time of FEBRUARY dataset is offset. Last record reads 3/24/21 9:47, but SHOULD READ 2/22/21 09:57

difftime(mdy_hms("3/24/21 9:47:05"), mdy_hms("2/22/21 09:57:58"), units = "secs")
# The Feb 2021 file is 2591347 seconds ahead. filename "R:/ltr_wetlands/main_project/data/water_budget/hoboFiles/raw data/202102_Feb-2021/W69_air_20210226.csv"

##TRY MANUALLY CORRECTING THIS, FOR NOW:

baro.compiled.corr <- baro.compiled %>%
  mutate(date.time.orig = if_else(filename == "R:/ltr_wetlands/main_project/data/water_budget/hoboFiles/raw data/202102_Feb-2021/W69_air_20210226.csv",
                                  date.time.orig-2591347, date.time.orig), # subtract offset in seconds
         date.time.est = case_when(gmt.offset == "05" ~ date.time.orig, # get all time stamps to the correct time (EST, not DST)
                                   gmt.offset == "04" ~ date.time.orig + (60*60),
                                   gmt.offset == "06" ~ date.time.orig - (60*60)),
         date.time.est = round_date(date.time.est, unit = "minute") ## do not need seconds-level temporal resolution
  )

baro.compiled.corr %>% 
  filter(filename == "R:/ltr_wetlands/main_project/data/water_budget/hoboFiles/raw data/202102_Feb-2021/W69_air_20210226.csv") %>% 
  arrange(date.time.est) %>% 
  select(site.id, date.time.est, date.time.orig, everything()) %>% 
  # head()
  tail()

# double check dups

dup.count <- baro.compiled.corr %>% 
  group_by(date.time.est, site.id, baro.pres_kpa, temp_c) %>% # identifies FULLY DUPLICATED data records
  add_count(name = "n.dup.recs") %>% 
  group_by(date.time.est, site.id) %>% # identifies ALL records where DATE-TIME is duplicated (includes records where RAW DATA DIFFERS)
  add_count(name = "n.dup.dttm") %>%
  filter(n.dup.dttm > n.dup.recs) %>% # filters out all records except those which erroneously appear as duplicates 
  group_by(filename) %>% 
  add_count(name = "n.dups.per.file")

### This worked!
# the only duplicate records are from time rounding, where coupler events occur at the same time as logged data. This is okay.
# but it would be best to find a better solution to address this when many files are offset.

#

#### VISUALIZE DATA

ggplot(baro.compiled.corr, aes(x = date.time.est, y = baro.pres_kpa))+
  geom_point() +
  facet_wrap( ~ site.id)

# separate the large file into two data sets: actual h2o lvl data and coupler event metadata

# h2o lvl data: just remove coupler event columns & lines with no h2o lvl data
kpa_only <- baro.compiled.corr %>% 
  select(site.id, date.time.est, baro.pres_kpa, temp_c, sensor.id, filename, date.time.orig) %>% 
  filter(!is.na(baro.pres_kpa) & !is.na(temp_c)) #%>% # remove records without pressure data
   
#check for duplicates
dups <- kpa_only %>%
  group_by(site.id, date.time.est) %>%
  add_count() %>%
  filter(n > 1)

### These are all dups from where datafiles overlap but share the same records/ raw data.
### These are dealt with simply by retaining one of the two dup files; generally the one from the earliest-collected data file.

# remove dups
kpa_only <- kpa_only %>% 
  arrange(date.time.est) %>%
  distinct(site.id, date.time.est, .keep_all = T) #Where records overlap and have the SAME DATA, keep only one of those records for each site and timestamp.

#check for duplicates again
dups <- kpa_only %>%
  group_by(site.id, date.time.est) %>%
  add_count() %>%
  filter(n > 1)
  
## ALL CLEAR!

### NEXT STEPS:
### Create a 1 min interpolated data set.
### But will need to filter out erroneous records before doing that.

## create a table of 1 minute kpa data, where kpa and temp are linearly interpolated between readings
## This should mimic the way onset matches barometric pressure and temp to water pressure data to calculate h2o levels
kpa_1min <- kpa_only %>% 
  mutate(YEAR = year(date.time.est),
         MONTH = month(date.time.est)) %>% 
  group_by(site.id, YEAR) %>%
  nest() #%>% 
  modifypad(interval = "min" , by = "date.time.est") 
  

# Now create a separate table of coupler events
# coupler event data are MESSY and often logged at slightly different times than the actual data record.
# Below, we match all coupler events with the closest data record in time that occured after the coupler event,
# and also consolidate multiple coupler events of the same type that were recorded within a ~15 minute time span.
kpa_coupler_evnts <- baro.compiled %>% 
  select(-c(date.time.est, abs.pres_kpa, temp_c, sensor.id, filename)) %>% #select only the coupler metadata, excluding messy time stamps
  filter(!is.na(coupler.detached) | !is.na(coupler.attached) | !is.na(host.connected) | !is.na(end.file) | !is.na(stopped) | !is.na(batt.v)) %>% #filter out records with NO coupler data recorded
  # mutate(batt.v = as.character(batt.v)) %>% 
  pivot_longer(., cols = c(batt.v, coupler.detached, coupler.attached, host.connected, end.file, stopped), names_to = "cplr_evnt", values_to = "logged") %>% # pivot longer to reduce duplication of information
  filter(!is.null(logged)) %>% 
  filter(!is.na(logged)) %>% #keep only logged events
  distinct() %>% # remove dups
  pivot_wider(names_from = "cplr_evnt", values_from = "logged") #return to wide format


kpa <- kpa_only %>% 
  left_join(kpa_coupler_evnts) %>%
  arrange(site.id, date.time) %>% 
  group_by(site.id) %>% 
  pad(by = "date.time") %>% 
  mutate(abs.pres_kpa = if_else(abs.pres_kpa < 105 & abs.pres_kpa > 95 & temp_c > -20 & temp_c < 50 , abs.pres_kpa, NA_real_),
         temp_c = if_else(temp_c > -20 & temp_c < 50, temp_c, NA_real_))

kpa

kpa.W69 <- kpa %>% 
  filter(site.id == "W69")

ggplot(kpa, aes(x = date.time, y = abs.pres_kpa, color = site.id)) +
  geom_point()+
  # ylim(min = 95, max = 105) +
  facet_wrap(~ site.id, ncol = 1)

# filter out all pressure values > 105 or < 95

ggplot(kpa, aes(x = date.time, y = temp_c, color = site.id)) +
  geom_point()+
  # ylim(min = 95, max = 105) +
  facet_wrap(~ site.id, ncol = 1)

# filter out all temp values > 50 or < -20

### Get a single baro datastream set up for joining to water level data
kpa.w69 <- kpa %>% 
  filter(site.id == "W69") %>% 
  rename(baro.site = site.id, baro.pres_kpa = abs.pres_kpa) %>% 
  select(baro.site, date.time, baro.pres_kpa)
