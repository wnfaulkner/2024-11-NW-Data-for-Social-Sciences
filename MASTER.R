#00000000000000000000000000000000000000000000000000000000000#
#0000       2024-11 NW Data for Social Sciences         0000#
#00000000000000000000000000000000000000000000000000000000000#

# 0-SETUP --------------------------------------------------------------------------------
	
  # INITIAL SETUP
    rm(list=ls()) #Remove lists
    gc()
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
      drive_auth(email = "william@fluxrme.com")
    library(purrr)
    library(ncdf4)
    library(janitor)
  
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
     
    
  # IMPORT SOURCE DATA (defining function) ----
  
    source.data.folder.id <- "16TJ2HhbdUzSEFatsTsOvtN3gYkM4eZDQ"
    import.files.tb <- 
      drive_ls(path = as_id(source.data.folder.id)) %>%
      mutate(mimeType = map_chr(drive_resource, "mimeType")) %>%
      filter(mimeType == "application/vnd.google-apps.spreadsheet") 
      
    ImportSourceData_GoogleSheets <- 
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
        
      } # End of function definition for ImportSourceData_GoogleSheets
    
    ImportSourceData_NC <- function(directory, num_files = NULL) {
      
      nc.dir <- directory
      nc.files.all <- list.files(nc.dir, pattern = "\\.nc$", full.names = TRUE)
      
      # Set num_files to the total number of files if not specified
      if (is.null(num_files)) {
        num_files <- length(nc.files.all)
      }
      
      # Select random subset of files if num_files is less than the total
      nc.files <- nc.files.all[sample(1:length(nc.files.all), min(num_files, length(nc.files.all)), replace = FALSE)]
      
      data.ls <- list()
      
      for (file in nc.files) {
        print(paste("Processing file:", file))
        nc <- nc_open(file)
        
        # Extract dimensions
        lat <- ncvar_get(nc, "lat")
        lon <- ncvar_get(nc, "lon")
        time <- ncvar_get(nc, "time")
        
        # Check if dimensions are valid
        if (is.null(lat) || is.null(lon) || is.null(time)) {
          warning(paste("Skipping file due to missing dimensions:", file))
          nc_close(nc)
          next
        }
        
        # Create a data frame for this file
        file.data <- expand.grid(lat = lat, lon = lon, time = time, file_name = basename(file)) %>% as_tibble()
        
        for (var.name in names(nc$var)) {
          var.data <- ncvar_get(nc, var.name)
          var.dims <- dim(var.data)
          
          print(paste("Variable:", var.name, "Dimensions:", paste(var.dims, collapse = " x ")))
          
          # Handle variables with 3D structure
          expected.dims <- c(length(lon), length(lat), max(1, length(time)))
          
          if (!is.null(var.data) && !is.null(var.dims)) {
            if (length(var.dims) == 3 && all(var.dims == expected.dims)) {
              # 3D variable: lon x lat x time
              file.data[[var.name]] <- as.vector(var.data)
            } else if (length(var.dims) == 2 && all(var.dims == c(length(lon), length(lat)))) {
              # 2D variable (e.g., no time dimension): replicate across time
              var.data <- array(var.data, dim = c(length(lon), length(lat), length(time)))
              file.data[[var.name]] <- as.vector(var.data)
            } else {
              warning(paste("Variable", var.name, "has unexpected dimensions. Skipping."))
            }
          } else {
            warning(paste("Variable", var.name, "is NULL or invalid. Skipping."))
          }
        }
        
        nc_close(nc)
        data.ls[[file]] <- file.data
      }
      
      return(data.ls)
    } # End of function definition for ImportSourceData_NC
    
# 2-CLEANING & RESHAPING --------------------------------------------------------------------------------
  
  #1. TEMPERATURE & PRECIPITATION ----
    
    # Define the function
      process_source_data <- function(name, directory, num_files) {
        # Step 1: Import files/tables to a list
        data_list <- ImportSourceData_NC(directory = directory, num_files = num_files)
        
        # Step 2: Clean names and combine into a single tibble
        data_table <- data_list %>%
          lapply(clean_names) %>% # Clean column names
          bind_rows() %>%         # Combine all tables into one large tibble
          as_tibble()
        
        # Return a list containing the table and its summary
        return(setNames(list(data_table), name))
      }
    
    # Directories and file information
      directories <- list(
        cntrl_03 = "G:\\.shortcut-targets-by-id\\1qZrp43j-iqzFcsNWVcnLqXYzfWOWn6xZ\\Data Paper\\1. Data & Figures\\1. Source Data\\1 & 2. Temperature & Precipitation\\nw_cntrl_03.TS_TSMN_TSMX_PRECC_PRECL.nc\\",
        targets_04 = "G:\\.shortcut-targets-by-id\\1qZrp43j-iqzFcsNWVcnLqXYzfWOWn6xZ\\Data Paper\\1. Data & Figures\\1. Source Data\\1 & 2. Temperature & Precipitation\\nw_targets_04.TS_TSMN_TSMX_PRECC_PRECL.nc.tar\\",
        targets_05 = "G:\\.shortcut-targets-by-id\\1qZrp43j-iqzFcsNWVcnLqXYzfWOWn6xZ\\Data Paper\\1. Data & Figures\\1. Source Data\\1 & 2. Temperature & Precipitation\\nw_targets_05.TS_TSMN_TSMX_PRECC_PRECL.nc.tar\\",
        ur_150 = "G:\\.shortcut-targets-by-id\\1qZrp43j-iqzFcsNWVcnLqXYzfWOWn6xZ\\Data Paper\\1. Data & Figures\\1. Source Data\\1 & 2. Temperature & Precipitation\\nw_targets_05.TS_TSMN_TSMX_PRECC_PRECL.nc.tar\\"
      )
    
    # Apply the function to all directories
      temp.precip.ls <- 
        lapply(
          names(directories), 
          function(name) {
            process_source_data(name = name, directory = directories[[name]], num_files = 3)
          }
        )
    
  #3. UV ----
    
    ImportSourceData_GoogleSheets("3. UV")
    
    CleanReshape_UV <- 
      function(source_table_list, source_table_names){
        
        theme <- "3.uv"
        
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
          select(-country.name) %>%
          melt(., id = c("country.id")) %>% #reshape to long
          mutate(
            climate.forcing.scenario = scenario,  #add scenario variable
            variable = variable %>% as.character, #convert variable made from column names of wide table from factor to character
            year = str_extract(variable, "^[^ ]+") %>% as.numeric,  #create year variable
            month = str_extract(variable, "(?<= - ).*") %>% as.numeric,  #create month variable
            indicator = indicator, #create indicator variable that tells us which indicator of UV we are looking at (e.g. UVA, UVB, UV Index, etc.)
            theme = theme
          ) %>%
          left_join( #add country metadata from configs table
            ., 
            countries.tb,
            by = "country.id"
          ) %>%
          left_join( #add associated publications metadata from configs table
            ., 
            associated.publications.tb,
            by = c("theme","climate.forcing.scenario")
          ) %>%
          left_join( #add months metadata (seasons in n & s hemisphere)
            ., 
            months.metadata.tb,
            by = "month"
          ) %>%
          select( #select & order final variables
            country.id, country.name, country.iso3,	country.hemisphere,	
            country.region,	country.sub.region,	country.intermediate.region, country.nuclear.weapons,
            climate.forcing.scenario, associated.publication_earth.system.simulation.reference,	associated.publication_analysis.and.discussion, 
            year, month, season.n.hemisphere, season.s.hemisphere,
            indicator, value
          ) %>% 
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
    
    #uv.clean.tb %>%
    #  select(-value) %>%
    #  apply(., 2, TableWithNA)
    
  #4a. AGRICULTURE CLM (Community Land Model) ----
    
    ImportSourceData_GoogleSheets("4a. Agriculture CLM")
    
    CleanReshape_AgricultureCLM <- 
      function(source_table_list, source_table_names){
        
        theme  <- "4a.agriculture"
        
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
          select(-id, -country.name) %>%
          melt(., id = "country.id") %>% #reshape to long
          mutate( #add/rename variables
            climate.forcing.scenario = variable %>% gsub("t","T", .),
            crop = crop,
            years_elapsed = years_elapsed,
            pct.change.harvest.mass = na_if(value, 9.96920996838686e+36),
            theme = theme 
          ) %>%
          left_join( #add country metadata from configs table
            ., 
            countries.tb,
            by = "country.id"
          ) %>%
          left_join( #add associated publications metadata from configs table
            ., 
            associated.publications.tb,
            by = c("theme","climate.forcing.scenario")
          ) %>%
          select( #select & order final variables
            country.id, country.name, country.iso3,	country.hemisphere,	
            country.region,	country.sub.region,	country.intermediate.region, country.nuclear.weapons, 
            climate.forcing.scenario, associated.publication_earth.system.simulation.reference,	associated.publication_analysis.and.discussion,
            years_elapsed,
            crop, 
            pct.change.harvest.mass
          ) %>% 
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
    
    #agriculture.clm.clean.tb %>% 
    #  select(-pct.change.harvest.mass) %>% 
    #  apply(., 2, TableWithNA) #display unique values for each variable except the indicator (for checking)
    
  #4b. AGRICULTURE MMA (Multi-Model Aggregates, Jonas) ----
    
    ImportSourceData_GoogleSheets("4b. Agriculture MMA")
    
    CleanReshape_AgricultureMMA <- 
      function(source_table_list, source_table_names){
        
        earth.system.simulation.reference  <- 
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
          select(-country_name, -country_iso3) %>%
          ReplaceNames(., "...1", "country.id") %>%  #standardize geographic variable names
          mutate(across(where(is.list), ~ suppressWarnings(as.character(unlist(.))))) %>% #convert all list variables into character
          melt(., id = "country.id") %>% #reshape to long
          mutate( #add/rename variables
            earth.system.simulation.reference  = earth.system.simulation.reference ,
            climate.forcing.scenario = scenario,
            crop = crop,
            years_elapsed = variable %>% str_extract(., "(?<=_)[^_]*$") %>% as.numeric,
            indicator = indicator,
            pct.change.harvest.yield = value %>% as.numeric %>% suppressWarnings()
          ) %>%
          left_join( #add country metadata from configs table
            ., 
            countries.tb,
            by = "country.id"
          ) %>%
          mutate(
            associated.publication_earth.system.simulation.reference = 
              recode(
                earth.system.simulation.reference,
                "Mills"="Mills et al., 2014, “Multidecadal Global Cooling and Unprecedented Ozone Loss Following a Regional Nuclear Conflict.”",
                "Bardeen"="Toon et al., 2019, “Rapidly Expanding Nuclear Arsenals in Pakistan and India Portend Regional and Global Catastrophe.”"
              ),
            associated.publication_analysis.and.discussion = "Jagermeyr et al., 2020, “A Regional Nuclear Conflict Would Compromise Global Food Security.”"
          ) %>%
          select( #select & order final variables
            country.id, country.name, country.iso3,	country.hemisphere,	
            country.region,	country.sub.region,	country.intermediate.region, country.nuclear.weapons, 
            climate.forcing.scenario, associated.publication_earth.system.simulation.reference, associated.publication_analysis.and.discussion,
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
    
    #agriculture.mma.clean.tb %>% 
    #  select(-names(.)[length(names(.))]) %>% 
    #  apply(., 2, TableWithNA) #display unique values for each variable except the indicator (for checking)
    
  #5. FISHERIES ----
    
    ImportSourceData_GoogleSheets("5. Fisheries")
    
    CleanReshape_Fisheries <- 
      function(source_table_list, source_table_names){
        
        theme <- "5.fisheries"
        
        scenario <- 
          source_table_names
        
        result <- 
          source_table_list %>% 
          select(names(.)[!str_detect(names(.), "ctrl")]) %>%
          ReplaceNames(., names(.),tolower(names(.))) %>% #lower-case all table names
          mutate(across(where(is.list), ~ suppressWarnings(as.numeric(unlist(.))))) %>% #convert all list variables into character
          melt(., id = c("eez","eez_no", "eez_area")) %>% #reshape to long
          mutate( #add/rename variables
            theme = theme,
            climate.forcing.scenario = scenario,
            years_elapsed = variable %>% str_extract(., "(?<=_)[^_]+$") %>% str_remove(., "yr") %>% as.numeric,
            indicator.raw = 
              variable %>% 
              str_extract(., "(?<=_).*?(?=_[^_]*$)"),
            value = value %>% divide_by(1000000000),
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
            climate.forcing.scenario = if_else(str_detect(indicator, "ctrl"), "control", climate.forcing.scenario),
          ) %>%
          dcast(
            ., 
            theme + climate.forcing.scenario + eez + eez_no + eez_area + years_elapsed ~ indicator,
            value.var = "value"
          ) %>%
          left_join( #add associated publications metadata from configs table
            ., 
            associated.publications.tb,
            by = c("theme","climate.forcing.scenario")
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
      mutate(
        mean.pct.catch.change = mean.pct.catch.change * 10^9,
        std.dev.pct.catch.change = std.dev.pct.catch.change * 10^9
      ) %>%
      select( #select & order final variables
        eez, eez_no, eez_area, 
        climate.forcing.scenario, associated.publication_earth.system.simulation.reference, associated.publication_analysis.and.discussion,
        years_elapsed, 
        mean.catch,  
        mean.catch.change, 
        mean.pct.catch.change, 
        std.dev.catch,
        std.dev.catch.change,
        std.dev.pct.catch.change
      ) 
    
    #fisheries.clean.tb %>%
    #  select(
    #    eez, eez_no, eez_area, 
    #    climate.forcing.scenario, associated.publication_earth.system.simulation.reference, associated.publication_analysis.and.discussion,
    #    years_elapsed
    #  ) %>%
    #  apply(., 2, TableWithNA)
    
  #6. SEA ICE ----
    
    ImportSourceData_GoogleSheets("6. Sea Ice")
    
    CleanReshape_SeaIce <- 
      function(source_table){
        
        scenario <- names(source_table)[1] %>% str_extract(., "(?<=NW-).*")
        
        theme  <- "6.sea.ice"
        
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
            month = (months_elapsed - 1) %% 12 + 1,  # Calculate the month (1-12)
            years_elapsed = (months_elapsed - 1) %/% 12 # Calculate the year (0, 1, 2, ...)
          ) %>%
          mutate(
            climate.forcing.scenario = scenario %>% recode(., "46.8Tg" = "47Tg"),
            theme = theme,
            indicator = "avg. thickness (m)"
          ) %>%
          left_join( #add months metadata (seasons in n & s hemisphere)
            ., 
            months.metadata.tb,
            by = "month"
          ) %>%
          left_join( #add associated publications metadata from configs table
            ., 
            associated.publications.tb,
            by = c("theme","climate.forcing.scenario")
          ) %>%
          select(
            port,
            climate.forcing.scenario, associated.publication_earth.system.simulation.reference, associated.publication_analysis.and.discussion, 
            month, months_elapsed, years_elapsed, 
            indicator, 
            sea.ice.thickness.meters
          ) %>%
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
    
    #sea.ice.clean.tb %>%
    #  select(-sea.ice.thickness.meters) %>%
    #  apply(., 2, TableWithNA)
  
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
