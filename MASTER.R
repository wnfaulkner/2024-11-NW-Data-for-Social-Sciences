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
  
    #Source Tables Directory (raw data, configs, etc.)
      source.tables.dir <- paste0(wd, "\\1. Source Data")
      if(dir.exists(source.tables.dir)){ 
        print("source.tables.dir exists.")
      }else{
        print("source.tables.dir DOES NOT EXIST.")
      }
      print(source.tables.dir)
  
  # LOAD LIBRARIES/PACKAGES
    library(wnf.utils)
    LoadCommonPackages()
    library(googledrive)
  
  # SECTION CLOCKING
    section0.duration <- Sys.time() - section0.starttime
    section0.duration
    
# 1-IMPORT --------------------------------------------------------------------------------
  
  # IMPORT CONFIG TABLES
    
    gs4_auth(email = "william@fluxrme.com")
    
    sheet.id = "https://docs.google.com/spreadsheets/d/1M9o6hIX9R8f44-UGea09Z27yhNhK340efd6Udgwrnl8/"
        
    configs.ss <-
      as_sheets_id(sheet.id)
      
    sheet.names.v <- sheet_names(configs.ss)
     
    all.configs.ls <-
      lapply(
        sheet.names.v, 
        function(x){
          read_sheet(configs.ss, sheet = x)
        }
      )
     
    names(all.configs.ls) <- sheet.names.v
     
    #Assign each table to its own tibble object
      ListToTibbleObjects(all.configs.ls) #Converts list elements to separate tibble objects names with
                                           #their respective sheet names with ".tb" appended
     
    #Extract global configs from tibble as their own character objects
      #TibbleToCharObjects(
      #   tibble = config.global.tb,
      #   object.names.colname = "config.name",
      #   object.values.colname = "config.value"
      # )
     
    
  # IMPORT SOURCE DATA
  
    source.data.folder.id <- "16TJ2HhbdUzSEFatsTsOvtN3gYkM4eZDQ"
    import.files.tb <- 
      drive_ls(path = as_id(source.data.folder.id)) %>%
      mutate(mimeType = map_chr(drive_resource, "mimeType")) %>%
      filter(mimeType == "application/vnd.google-apps.spreadsheet") #%>%
      #select(id) %>%
      #unlist %>% as.vector
    
    i=1
    #for(i in 1:nrow(import.files.tb)){ # Start of loop i by imported file (Google Sheet)
      
      file.id.i <- import.files.tb$id[i] %>% as_sheets_id(.)
      sheet.names.i <- sheet_names(file.id.i)
      configs.i <- source.table.configs.tb %>% filter(file.name == import.files.tb[i]$name)
      
      import.tables.ls <- 
        lapply(
          sheet.names.i, 
          function(x){
            read_sheet(file.id.i, sheet = x)
          }
        )
      
    #} # End of loop i by imported file (Google Sheet)
    
############
    #setwd(source.tables.dir)
    #import.filename <- "Bardeen_5Tg_maize_production_change_country_yr1-15_multi_model_mean_2022-08-22.csv"
    #data.tb <- read.csv(import.filename) %>% as_tibble
###########
    
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
    
  # CODE CLOCKING
    code.duration <- Sys.time() - code.starttime
    code.duration
  
    
    
    
    
    
    
    
    
    
    
    
    