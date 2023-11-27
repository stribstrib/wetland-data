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



# FUNCTION TO IMPORT AND COMBINE RAW DATA --------------------

# create a function that reads in lai files (.txt) consistently and adds original filename to each record.

read.files <- function(flpth){
  
  # pulls the first row in each file containing column names and creates r-friendly names for further processing of metadata (sensor num, units) 
  header1 <- names(read_csv(flpth, n_max = 0)) %>%
    make_clean_names(use_make_names = F, 
                     parsing_option = 0,
                     replace = c(`\\(` = "_", `\\)` = "_", `#` = "", `°` = "_"))
  
  # extract the units of temp (can vary, depending on how logger was launched)  
  temp_units <- header1 %>%
    str_extract(., "(?<=temp\\.).") %>% 
    na.omit()
  
  #extract units of pressure (varies depending on how logger was launched)
  pres_units <- header1 %>%
    str_extract(., "(?<=abs.pres_)...") %>% 
    na.omit()
  
  # extract 8-digit sensor serial number
  sensor_num <- header1 %>% 
    str_extract(., "(?<=end.of_file_)........") %>% 
    na.omit()
  
  # clean up the header now that we've gotten all the metadata out that we need. 
  header2 <- header1 %>% 
    str_replace_all(., "[:digit:]", "") %>% #remove all digits (i.e. sensor serial numbers) 
    str_replace_all(., "_psi_|_kpa_|.c_|.f_", "") %>% #remove units
    str_replace_all(., "_$|\\.$", "") %>%  #remove underscores and periods at the ends of col names
    str_replace_all(., "_", ".") #replace last _ with "." so all names are consistent
   
  
  read_csv(flpth, skip = 1, col_names = header2, col_types = cols(.default = "c")) %>%  # script for reading a file into R and adding filename and datalogger program as metadata columns
    # select(date.time:end.of_file) %>% 
    mutate(deg_unit = temp_units[1],
           pres_unit = pres_units[1],
           sensor_id = sensor_num[1],
           site_id = "W69",
           filename = flpth #basename(flpth)
           )
}

# read.files <- function(flnm){
#   read_tsv(flnm) %>%  # script for reading tab separated value files into R, ensuring consistent varaible types, and adding filename as metadata column
#     clean_names() %>% 
#     mutate(filename = flnm)
# }
# 

# define specific pattern with which all files we're interested in can be located
air.filename.pattern <- "*air*.csv" 

# load all files in that match the above pattern, using the read.filed function.
#includes creating a column for the original timestammp on senor records (often offset) and the adjusted timestamp every 15 mins, so all sensors can be merged easily
baro.compiled <-                                  
  list.files(path = c(hobo.data.fp),
             full.names = TRUE,
             pattern = glob2rx(air.filename.pattern),
             recursive = T)  %>% 
  str_subset(., "CORRECTED DATA", negate = TRUE) %>% # exclude files from the "CORRECTED DATA" folder; I believe these have been processed?
  map_df(~read.files(.)) #%>%
  mutate(date.time = mdy_hms(date.time),
         abs.pres = as.numeric(abs.pres),
         temp = as.numeric(temp)
  rename(date.time.orig = date.time) %>% 
  arrange(date.time.orig) %>% 
  select(site_id, date.time.orig, abs.pres.kpa:end.of.file, stopped, sensor_id, filename) %>% 
  distinct(., pick(site_id:sensor_id), .keep_all = T) %>% 
  mutate(date.time = round_date(date.time.orig, unit = "15 mins"))

# separate the large file into two data sets: actual h2o lvl data and coupler event metadata

# h2o lvl data: just remove coupler event columns & lines with no h2o lvl data
h2olvl <- h2olvl.compiled %>% 
  select(date.time, site_id, abs.pres.kpa, temp.c, sensor_id, filename, date.time.orig) %>% 
  filter(!is.na(abs.pres.kpa) & !is.na(temp.c))

