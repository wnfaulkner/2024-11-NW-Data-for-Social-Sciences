#00000000000000000000000000000000000000000000000000000000000#
#0000       2024-11 NW Data for Social Sciences         0000#
#00000000000000000000000000000000000000000000000000000000000#

# 0-SETUP --------------------------------------------------------------------------------
	
  # INITIAL SETUP
    rm(list=ls()) #Remove lists
    options(java.parameters = "- Xmx8g") #helps r not to fail when importing large xlsx files with xlsx package
    
  # SECTION & CODE CLOCKING
    
    sections.all.starttime <- Sys.time()
    section0.starttime <- sections.all.starttime
  
  # ESTABLISH BASE DIRECTORIES
  
    # Figure out what machine code is running on
      nodename <- Sys.info()[which(row.names(as.data.frame(Sys.info())) == "nodename")]
      
      if(nodename == "LAPTOP-NIDLDLA7"){
        machine <- "t470"
      }else{}
      
      if(nodename == "WNF-T16"){
        machine = "t16"
      }else{}
    
    # Set Working Directory and R Project Directory
      if(machine == "t16"){  
        
        #T-16
          wd <- "G:\\.shortcut-targets-by-id\\1qZrp43j-iqzFcsNWVcnLqXYzfWOWn6xZ\\Data Paper\\1. Data & Figures"
          rproj.dir <- "C:\\Users\\willi\\OneDrive\\Documents\\GIT PROJECTS\\2024-11-NW-Data-for-Social-Sciences"
      
      }else{
        
        #Thinkpad T470
          wd <- ""
          rproj.dir <- ""
          
      }
    
    # Check Directories
      wd <- if(!dir.exists(wd)){choose.dir()}else{wd}
      rproj.dir <- if(!dir.exists(rproj.dir)){choose.dir()}else{rproj.dir}
  
    # Source Tables Directory (raw data, configs, etc.)
      source.tables.dir <- paste0(wd, "\\NW OSF Human Impact DB\\Agriculture\\Multi-model aggregation (Jonas)")
      if(dir.exists(source.tables.dir)){ 
        print("source.tables.dir exists.")
      }else{
        print("source.tables.dir DOES NOT EXIST.")
      }
      print(source.tables.dir)
  
  # LOAD LIBRARIES/PACKAGES
    library(wnf.utils)
    LoadCommonPackages()
  
  # SECTION CLOCKING
    section0.duration <- Sys.time() - section0.starttime
    section0.duration
    
# 1-IMPORT --------------------------------------------------------------------------------

  setwd(source.tables.dir)
  import.filename <- "Bardeen_5Tg_maize_production_change_country_yr1-15_multi_model_mean_2022-08-22.csv"
  data.tb <- read.csv(import.filename) %>% as_tibble
    
# 2-Cleaning --------------------------------------------------------------------------------
  
  names(data.tb) %<>% tolower
  
# 3-Reshaping --------------------------------------------------------------------------------
  
  crop.i <- import.filename %>% strsplit(., "_") %>% unlist %>% .[3]
    #ifelse(
    #  strsplit(., "_") %>% unlist %>% length %>% is_weakly_greater_than(3),
    #  strsplit(., "_") %>% unlist %>% .[3]
    #)
  
  data.tb %<>%
    select(-x) %>%
    melt(., id = c("country_name", "country_iso3")) %>%
    mutate(
      ., 
      crop = crop.i,
      year = 
        variable %>% 
        as.character %>% 
        str_extract(., "(?<=_)[^_]*$") %>%
        as.numeric,  
    ) %>%
    select(-variable) %>%
    as_tibble
  
# 4-Export --------------------------------------------------------------------------------
  
  setwd(wd)
  
  #Define/Create Output Directory
    output.base.name <- 
      Sys.time() %>% 
      gsub(":",".",.) #%>% 
      #paste("output ", ., sep = "")
      
    output.dir <-
      paste(
        wd,
        "\\Reformatted Data Outputs\\",
        output.base.name,
        "\\",
        sep = ""
      )
    
    if(output.dir %>% dir.exists %>% not){
      dir.create(output.dir, recursive = TRUE)
    }
    
  #Define Output Filename
    output.csv.filename <- 
      paste("output_", output.base.name, ".csv", sep = "")
    
  #Store File
    setwd(output.dir)
    write.csv(data.tb, output.csv.filename)
  
    
    
    
    
    
    
    
    
    
    
    
    
