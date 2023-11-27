### READ IN FILES PROCESSED IN HOBOWARE TO TEST AGAINST R VALUES



### MODIFIED SCRIPT TO READ IN FILES
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
    str_replace_all(., ".lgr.s/n:........", "") %>%
    str_replace_all(., ",meters", "_m") %>% 
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
    select(site.id, date.time.orig = date.time, gmt.offset, abs.pres, pres.units, temp, temp.units, sensor.id, water.level.m, filename, everything())
  
  
}


# FILEPATH FOR PROCESSED DATA
hobo.processed.fp <- "R:/ltr_wetlands/main_project/data/water_budget/hoboFiles/processed data/trial_h2olvl_via_hoboware_2021-05_dwnld"

flpth <- "R:/ltr_wetlands/main_project/data/water_budget/hoboFiles/processed data/trial_h2olvl_via_hoboware_2021-05_dwnld/FP02_H2Olvl_20210527.csv"

### READ IN AND COMPILE
h2olvl.processed.compiled <-
  list.files(path = hobo.processed.fp,
             recursive = TRUE,
             full.names = TRUE,
             pattern = H2Olvl.filename.pattern) %>%  
  map_df(~read.files(.)) %>%
  rename(water.level_m = water.level.m) %>% 
  filter(!is.na(water.level_m)) %>% 
  mutate(date.time.orig = mdy_hms(date.time.orig),
         abs.pres = as.numeric(abs.pres),
         temp = as.numeric(temp),
         water.level_m = as.numeric(water.level_m),
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
  select(site.id:sensor.id, abs.pres_kpa, temp_c, water.level_m, everything()) %>% 
  arrange(date.time.est) %>%
  distinct(., pick(site.id:water.level_m), .keep_all = T) %>%
  mutate(date.time = round_date(date.time.est, unit = "15 mins")) %>% 
  select(-c(abs.pres, pres.units, temp, temp.units, date.time.orig, gmt.offset)) %>% 
  select(site.id, date.time, water.level_m, abs.pres_kpa, temp_c, sensor.id, filename, everything())

