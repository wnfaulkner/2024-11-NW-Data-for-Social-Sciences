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
    library(purrr)
  
  # SECTION CLOCKING
    section0.duration <- Sys.time() - section0.starttime
    section0.duration
    
# 1-IMPORT --------------------------------------------------------------------------------
  
  # IMPORT CONFIG TABLES ----
    
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
     
    
  # IMPORT SOURCE DATA (defining function only) ----
  
    source.data.folder.id <- "16TJ2HhbdUzSEFatsTsOvtN3gYkM4eZDQ"
    import.files.tb <- 
      drive_ls(path = as_id(source.data.folder.id)) %>%
      mutate(mimeType = map_chr(drive_resource, "mimeType")) %>%
      filter(mimeType == "application/vnd.google-apps.spreadsheet") 
      
    ImportSourceData <- 
      function(name_of_file_to_be_imported){
        
        file.id <- 
          import.files.tb %>% 
          filter(name == name_of_file_to_be_imported) %>% 
          select(id) %>%
          unlist %>% as.vector() %>%
          as_sheets_id(.)
        
        sheet.names <- sheet_names(file.id)
        
        configs <- source.table.configs.tb %>% filter(file.name == name_of_file_to_be_imported)
        
        import.tables.ls <- 
          lapply(
            sheet.names, 
            function(x){
              read_sheet(file.id, sheet = x)
            }
          )
        
        names(import.tables.ls) <- sheet.names #assign sheet names as list element names
        
        list.name <- configs$table.name %>% paste0(., ".ls", collapse = "")
        assign(list.name, import.tables.ls, envir = .GlobalEnv) #create a list of the imported tables in the global environment
        print(list.name)
        
      } # End of function definition for ImportSourceData
    
# 2-CLEANING & RESHAPING --------------------------------------------------------------------------------
  
  #1. TEMPERATURE ----
    
  #2. PRECIPITATION ----
    
  #3. UV ----
    
    ImportSourceData("3. UV")
    
    CleanReshape_UV <- 
      function(source_table_list, source_table_names){

        scenario <- 
          source_table_names %>%
          strsplit(., "_") %>% 
          unlist %>%
          .[1] %>%
          ifelse(. != "control", paste(., "Tg", sep=""), .)
        
        indicator <- 
          source_table_names %>%
          strsplit(., "_") %>% 
          unlist %>%
          .[2]
        
        result <- 
          source_table_list %>% 
          ReplaceNames(., names(.),tolower(names(.))) %>% #lower-case all table names
          ReplaceNames(., c("id", "nation"), c("country.id","country.name")) %>%  #standardize geographic variable names
          mutate(across(where(is.list), ~ suppressWarnings(as.numeric(unlist(.))))) %>% #convert all list variables into numeric
          melt(., id = c("country.id","country.name")) %>% #reshape to long
          mutate(
            scenario = scenario,  #add scenario variable
            variable = variable %>% as.character, #convert variable made from column names of wide table from factor to character
            year = str_extract(variable, "^[^ ]+"),  #create year variable
            month = str_extract(variable, "(?<= - ).*"),  #create month variable
            indicator = indicator #create indicator variable that tells us which indicator of UV we are looking at (e.g. UVA, UVB, UV Index, etc.)
          ) %>%
          select(country.id, country.name, scenario, year, month, indicator, value) %>% #select & order final variables
          as_tibble  #ensure final result is a tibble
        
        print(source_table_names)
        
        return(result)
      }
    
    uv.clean.tb <-
      Map(
          CleanReshape_UV,
          uv.ls,
          names(uv.ls)
      ) %>%
      do.call(rbind, .) %>%
      as_tibble()
    
  #4a. AGRICULTURE CLM (Community Land Model, Lili) ----
    
    ImportSourceData("4a. Agriculture CLM")
    
    CleanReshape_AgricultureCLM <- 
      function(source_table_list, source_table_names){
        
        crop <- 
          source_table_names %>%
          strsplit(., "-") %>% 
          unlist %>%
          .[1]
        
        years_elapsed <- 
          source_table_names %>%
          strsplit(., "-") %>% 
          unlist %>%
          .[2]
        
        result <- 
          source_table_list %>% 
          ReplaceNames(., names(.),tolower(names(.))) %>% #lower-case all table names
          ReplaceNames(., c("nation-id", "nation-name"), c("country.id","country.name")) %>%  #standardize geographic variable names
          select(-id) %>%
          melt(., id = c("country.id","country.name")) %>% #reshape to long
          mutate( #add/rename variables
            scenario = variable,
            crop = crop,
            years_elapsed = years_elapsed,
            pct.change.harvest.mass = value
          ) %>%
          select(country.id, country.name, scenario, crop, years_elapsed, pct.change.harvest.mass) %>% #select & order final variables
          as_tibble #ensure final result is a tibble
        
        print(source_table_names)
        
        return(result)
      }
    
    agriculture.clm.clean.tb <- #create final cleaned & compiled data table
      Map(
        CleanReshape_AgricultureCLM,
        agriculture.clm.ls,
        names(agriculture.clm.ls)
      ) %>%
      do.call(rbind, .) %>%
      as_tibble()
    
    agriculture.clm.clean.tb %>% 
      select(-pct.change.harvest.mass) %>% 
      apply(., 2, TableWithNA) #display unique values for each variable except the indicator (for checking)
    
  #4b. AGRICULTURE MMA (Multi-Model Aggregates, Jonas) ----
    
    ImportSourceData("4b. Agriculture MMA")
    
    CleanReshape_AgricultureMMA <- 
      function(source_table_list, source_table_names){
        
        climate.model <- 
          source_table_names %>%
          strsplit(., "_") %>% 
          unlist %>%
          .[1]
        
        scenario <- 
          source_table_names %>%
          strsplit(., "_") %>% 
          unlist %>%
          .[2]
        
        crop <- 
          source_table_names %>%
          strsplit(., "_") %>% 
          unlist %>%
          .[3]
        
        indicator <- "% change in harvest yield (tons/hectare)"
        
        result <- 
          source_table_list %>% 
          ReplaceNames(., names(.),tolower(names(.))) %>% #lower-case all table names
          ReplaceNames(., c("...1","country_name", "country_iso3"), c("country.id","country.name","country.abbrev.iso3")) %>%  #standardize geographic variable names
          mutate(across(where(is.list), ~ suppressWarnings(as.character(unlist(.))))) %>% #convert all list variables into character
          melt(., id = c("country.id","country.name", "country.abbrev.iso3")) %>% #reshape to long
          mutate( #add/rename variables
            climate.model = climate.model,
            scenario = scenario,
            crop = crop,
            years_elapsed = variable %>% str_extract(., "(?<=_)[^_]*$"),
            indicator = indicator,
            pct.change.harvest.yield = value %>% as.numeric %>% suppressWarnings()
          ) %>%
          select( #select & order final variables
            climate.model, scenario, 
            country.id, country.name, country.abbrev.iso3, 
            years_elapsed, 
            crop, indicator, pct.change.harvest.yield
          ) %>% 
          as_tibble #ensure final result is a tibble
        
        print(source_table_names)
        
        return(result)
      }
    
    agriculture.mma.clean.tb <- #create final cleaned & compiled data table
      Map(
        CleanReshape_AgricultureMMA,
        agriculture.mma.ls,
        names(agriculture.mma.ls)
      ) %>%
      do.call(rbind, .) %>%
      as_tibble()
    
    agriculture.mma.clean.tb %>% 
      select(-names(.)[length(names(.))]) %>% 
      apply(., 2, TableWithNA) #display unique values for each variable except the indicator (for checking)
    
  #5. FISHERIES ----
    
    ImportSourceData("5. Fisheries")
    
    CleanReshape_Fisheries <- 
      function(source_table_list, source_table_names){
        
        climate.model <- ""
        
        scenario <- 
          source_table_names
        
        result <- 
          source_table_list %>% 
          select(names(.)[!str_detect(names(.), "ctrl")]) %>%
          ReplaceNames(., names(.),tolower(names(.))) %>% #lower-case all table names
          mutate(across(where(is.list), ~ suppressWarnings(as.numeric(unlist(.))))) %>% #convert all list variables into character
          melt(., id = c("eez","eez_no", "eez_area")) %>% #reshape to long
          mutate( #add/rename variables
            climate.model = climate.model,
            years_elapsed = variable %>% str_extract(., "(?<=_)[^_]+$") %>% str_remove(., "yr") %>% as.numeric,
            indicator.raw = 
              variable %>% 
              str_extract(., "(?<=_).*?(?=_[^_]*$)"),
            scenario = scenario,
          ) %>%
          mutate(
            indicator = 
              IndexMatchToVectorFromTibble(
                indicator.raw, 
                fisheries.indicators.tb,
                "extracted.indicator.name.raw",
                "indicator.name.clean",
                mult.replacements.per.cell = FALSE
              ),
            scenario = if_else(str_detect(indicator, "ctrl"), "control", scenario),
          ) %>%
          dcast(
            ., 
            climate.model + scenario + eez + eez_no + eez_area + years_elapsed ~ indicator,
            value.var = "value"
          ) %>%
          as_tibble #ensure final result is a tibble
        
        print(source_table_names)
        
        return(result)
      }
    
    fisheries.clean.tb <- #create final cleaned & compiled data table
      Map(
        CleanReshape_Fisheries,
        fisheries.ls,
        names(fisheries.ls)
      ) %>%
      bind_rows(.) %>%
      select( #select & order final variables
        climate.model, scenario, 
        eez, eez_no, eez_area, 
        years_elapsed, 
        mean.catch, std.dev.catch, 
        mean.catch.change, std.dev.catch.change,
        pct.catch.change, std.dev.pct.catch.change
      ) #%>%
      #select(climate.model, scenario, eez, eez_no, eez_area,years_elapsed) %>%
      #apply(., 2, TableWithNA)
    
  #6. SEA ICE ----
    
    ImportSourceData("6. Sea Ice")
    
    #source_table <- sea.ice.ls[[2]] # for testing
    CleanReshape_SeaIce <- 
      function(source_table){
        
        scenario <- names(source_table)[1] %>% str_extract(., "(?<=NW-).*")
        
        result <-
          source_table %>%
          .[-1,] %>%
          ReplaceNames(., names(source_table)[1], "port") %>%
          melt(
            .,
            id = "port"
          ) %>%
          ReplaceNames(., c("variable","value"), c("month","sea.ice.thickness.meters")) %>%
          mutate(
            months_elapsed = as.character(month) %>% gsub("\\.", "", .) %>% as.numeric %>% subtract(1),  # Clean and convert month strings
            month = (months_elapsed - 1) %% 12 + 1,                                     # Calculate the month (1-12)
            years_elapsed = (months_elapsed - 1) %/% 12                                 # Calculate the year (0, 1, 2, ...)
          ) %>%
          mutate(
            scenario = scenario,
            indicator = "avg. thickness (m)"
          ) %>%
          select(port,scenario, month, months_elapsed, years_elapsed, indicator, sea.ice.thickness.meters) %>%
          as_tibble
        
        return(result)
      }
    
    sea.ice.clean.tb <-
      lapply(
        sea.ice.ls,
        CleanReshape_SeaIce
      ) %>%
      do.call(rbind, .) %>%
      as_tibble()
  
# 4-EXPORT --------------------------------------------------------------------------------
  
  # CREATE FINAL LIST OF CLEANED & REFORMATTED TABLES FOR EXPORT
    
    clean_object_names <- 
      c(
        "temperature.clean.tb",
        "precipitation.clean.tb",
        "uv.clean.tb",
        "agriculture.clm.clean.tb",
        "agriculture.mma.clean.tb",
        "fisheries.clean.tb",
        "sea.ice.clean.tb"
      )
    
    clean_table_names <- 
      c(
        "1.temperature",
        "2.precipitation",
        "3.uv",
        "4a.agriculture.clm",
        "4b.agriculture.mma",
        "5.fisheries",
        "6.sea.ice"
      )
    
    export.ls <- 
      lapply(
        clean_object_names, 
        function(x) {
          if (exists(x)) get(x) else NULL
        }
      ) %>%
      purrr::compact() # Remove NULL entries for non-existent tibbles
    
    names(export.ls) <- clean_table_names[clean_object_names %in% ls()]
  
  # DEFINE & CREATE OUTPUT DIRECTORY
    
    #setwd(paste(wd, "\\2. Reformatted Source Data", sep=""))
    
    output.base.name <- 
      Sys.time() %>% 
      gsub(":",".",.) 
      
    output.dir <-
      paste(
        wd,
        "\\2. Reformatted Source Data\\",
        output.base.name,
        "\\",
        sep = ""
      )
    
    if(output.dir %>% dir.exists %>% not){
      dir.create(output.dir, recursive = TRUE)
    }
    
  # WRITE CSV FILES INTO OUTPUT DIRECTORY
    
    ExportCsvs <- 
      function(table, table_name){
        file.name <- paste(table_name,"_",output.base.name,".csv",sep="")
        write.csv(table, file.name, row.names = FALSE, na = "")
      }
    
    setwd(output.dir)
    Map(
      ExportCsvs,
      export.ls,
      names(export.ls)
    )

  # CODE CLOCKING
    code.duration <- Sys.time() - sections.all.starttime
    code.duration
