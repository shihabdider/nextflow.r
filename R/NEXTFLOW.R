library(R6)
library(data.table)
library(digest) # for creating a hash

# Defining the Run object
Run <- R6Class("Run",
  public = list(
    pipeline_path = NULL,
    run_path = NULL,
    name = NULL,
    nextflow_parameters = data.table(),
    tool_parameters = data.table(),
    initialize = function(pipeline_path, run_path, name = NULL) {
      self$pipeline_path <- pipeline_path
      self$run_path <- run_path
      self$name <- ifelse(is.null(name), digest(run_path), name)

      # Create the data.table with the correct column order
      self$nextflow_parameters <- data.table(
        parameter_name = character(),
        value = logical(),
        flag = logical(),
        desc = character()
      )

      # Populate the data.table with parameter data
      self$nextflow_parameters <- rbindlist(list(
        list("-E", FALSE, TRUE, "Exports all current system environment"),
        list("-ansi-log", NULL, TRUE, "Enable/disable ANSI console logging"),
        list("-bucket-dir", NULL, FALSE, "Remote bucket where intermediate result files are stored"),
        list("-cache", NULL, TRUE, "Enable/disable processes caching"),
        list("-disable-jobs-cancellation", NULL, TRUE, "Prevent the cancellation of child jobs on execution termination"),
        list("-dsl1", FALSE, TRUE, "Execute the workflow using DSL1 syntax"),
        list("-dsl2", FALSE, TRUE, "Execute the workflow using DSL2 syntax"),
        list("-dump-channels", NULL, TRUE, "Dump channels for debugging purpose"),
        list("-dump-hashes", FALSE, TRUE, "Dump task hash keys for debugging purpose"),
        list("-e.", list(), FALSE, "Add the specified variable to execution environment"),
        list("-entry", NULL, FALSE, "Entry workflow name to be executed"),
        list("-h, -help", FALSE, TRUE, "Print the command usage"),
        list("-hub", NULL, FALSE, "Service hub where the project is hosted"),
        list("-latest", FALSE, TRUE, "Pull latest changes before run"),
        list("-lib", NULL, FALSE, "Library extension path"),
        list("-main-script", NULL, FALSE, "The script file to be executed when launching a project directory or repository"),
        list("-offline", FALSE, TRUE, "Do not check for remote project updates"),
        list("-params-file", NULL, FALSE, "Load script parameters from a JSON/YAML file"),
        list("-plugins", NULL, FALSE, "Specify the plugins to be applied for this run e.g. nf-amazon,nf-tower"),
        list("-preview", FALSE, TRUE, "Run the workflow script skipping the execution of all processes"),
        list("-process.", list(), FALSE, "Set process options"),
        list("-profile", NULL, FALSE, "Choose a configuration profile"),
        list("-queue-size", NULL, FALSE, "Max number of processes that can be executed in parallel by each executor"),
        list("-resume", NULL, TRUE, "Execute the script using the cached results, useful to continue"),
        list("-revision", NULL, FALSE, "Revision of the project to run (either a git branch, tag or commit SHA number)"),
        list("-stub-run", FALSE, TRUE, "Execute the workflow replacing process scripts with command stubs"),
        list("-test", NULL, FALSE, "Test a script function with the name specified"),
        list("-user", NULL, FALSE, "Private repository user name"),
        list("-with-apptainer", NULL, TRUE, "Enable process execution in a Apptainer container"),
        list("-with-charliecloud", NULL, TRUE, "Enable process execution in a Charliecloud container runtime"),
        list("-with-conda", NULL, TRUE, "Use the specified Conda environment package or file"),
        list("-with-dag", NULL, TRUE, "Create pipeline DAG file"),
        list("-with-docker", NULL, TRUE, "Enable process execution in a Docker container"),
        list("-with-notification", NULL, TRUE, "Send a notification email on workflow completion to the specified recipients"),
        list("-with-podman", NULL, TRUE, "Enable process execution in a Podman container"),
        list("-with-singularity", NULL, TRUE, "Enable process execution in a Singularity container"),
        list("-with-spack", NULL, TRUE, "Use the specified Spack environment package or file"),
        list("-with-tower", NULL, TRUE, "Monitor workflow execution with Seqera Tower service"),
        list("-with-weblog", NULL, TRUE, "Send workflow status messages via HTTP to target URL"),
        list("-without-conda", NULL, TRUE, "Disable the use of Conda environments"),
        list("-without-docker", FALSE, TRUE, "Disable process execution with Docker"),
        list("-without-podman", NULL, TRUE, "Disable process execution in a Podman container"),
        list("-without-spack", NULL, TRUE, "Disable the use of Spack environments"),
        list("-work-dir", NULL, FALSE, "Directory where intermediate result files are stored")
      ), fill = TRUE)

      # tool parameters
      schema_path <- file.path(self$pipeline_path, "nextflow_schema.json")
      if (file.exists(schema_path)) {
        nextflow_schema <- jsonlite::fromJSON(schema_path, simplifyVector = FALSE)
      } else {
        stop("nextflow_schema.json not found in pipeline_path")
      }

      extract_tool_parameters <- function(tool_parameters) {
        results_dt <- data.table(parameter = character(), value = character(), type = character(), desc = character())
        # Check if the 'definitions' attribute exists
        if ("definitions" %in% names(tool_parameters)) {
          # Iterate over the 'definitions'
          for (definition_name in names(tool_parameters$definitions)) {
            definition <- tool_parameters$definitions[[definition_name]]
            # Check if 'properties' attribute exists in the current definition
            if ("properties" %in% names(definition)) {
              properties <- definition$properties
              # Iterate over 'properties'
              for (property_name in names(properties)) {
                property <- properties[[property_name]]
                # Check if the 'default' attribute exists in the current property
                if ("default" %in% names(property)) {
                  # Extract other attributes as needed and add to the data table
                  default_value <- property$default

                  if (property_name == "validate_params") {
                    next
                  }
                  type <- property$type # As an example
                  desc <- property$help_text
                  # Add the results as a new row to the data table
                  results_dt <- rbind(results_dt, list(
                    parameter = property_name,
                    value = default_value,
                    type = type,
                    desc = desc
                  ), fill = TRUE)
                }
              }
            }
          }
        }
        return(results_dt)
      }

      self$tool_parameters <- extract_tool_parameters(nextflow_schema)
    },
    execute = function(samplesheet) {
      starting_step = self$tool_parameters[self$tool_parameters$parameter == "step", "value"]

      # Define a list holding required columns with descriptions
      column_descriptions <- list(
        patient = "Patient or Sample ID. Each patient can have multiple sample names.",
        sample = "Sample ID for each Patient. Sample IDs should be unique to Patient IDs.",
        fastq_1 = "Full path to FASTQ file read 1. Required for --step alignment.",
        fastq_2 = "Full path to FASTQ file read 2. Required for --step alignment.",
        bam = "Full path to BAM file. Required for --step sv_calling, fragcounter or hetpileups.",
        cov = "Full path to Coverage file. Required for --step dryclean, ascat, or jabba",
        hets = "Full path to HetPileups .txt file. Required for --step ascat or jabba",
        vcf = "Full path to VCF file. Required for --step jabba.",
        vcf2 = "Full path to VCF file with unfiltered somatic SVs. Required for --step jabba.",
        seg = "Full path to .rds file of GRanges object of intervals corresponding to initial segmentation Required for --step jabba.",
        nseg = "Full path to .rds file of GRanges object of intervals corresponding to normal tissue copy number, needs to have $cn field. Required for --step jabba."
      )

      # Define required columns based on starting_step
      required_columns <- c("patient", "sample")
      step_required_columns <- switch(starting_step,
        "alignment" = c("fastq_1", "fastq_2"),
        "sv_calling" = "bam",
        "fragcounter" = "bam",
        "hetpileups" = "bam",
        "dryclean" = "cov",
        "ascat" = c("cov", "hets"),
        "jabba" = c("cov", "hets", "vcf", "vcf2", "seg", "nseg")
      )
      required_columns <- c(required_columns, step_required_columns)

      # Check for missing required columns
      missing_columns <- setdiff(required_columns, names(samplesheet))

      if (length(missing_columns) > 0) {
        message("Samplesheet is missing required columns for step '", starting_step, "': ", paste(missing_columns, collapse = ", "))
        message("Column descriptions:")
        for (col in missing_columns) {
          message("* ", col, ": ", column_descriptions[[col]])
        }
        message("Returning empty data.table with required samplesheet columns...")
        empty_dt <- data.table(setNames(nm = lapply(required_columns, function(col) character())))
        return(empty_dt)
      }

      # Samplesheet requires bai column as well
      # Assumes each bai is in the same directory and has the same filename as its bam
      has_bam <- "bam" %in% names(samplesheet)
      if (has_bam) {
        samplesheet$bai <- paste0(samplesheet$bam, ".bai")
      }

      # Continue with execution if all required columns are present...
      samplesheet_path <- file.path(self$run_path, "samplesheet.csv")
      fwrite(samplesheet, samplesheet_path)
      message(paste("Wrote samplesheet to", samplesheet_path))

      current_timestamp <- Sys.time()
      formatted_timestamp <- format(current_timestamp, "%Y-%m-%d %H:%M:%S")
      run_name <- paste("run_", formatted_timestamp)
      self$name <- run_name

      cmd <- paste(
        "nextflow -bg run",
        shQuote(self$pipeline_path),
        "-with-report",
        "-with-trace",
        "-with-timeline",
        paste("-name ", self$name),
        paste("--outdir", shQuote(self$run_path)),
        paste("--input", shQuote(samplesheet_path))
      )

      # Assuming self$tool_parameters is a data.table with 'parameter' and 'value' columns
      for (i in seq_len(nrow(self$tool_parameters))) {
        param <- self$tool_parameters[i, ]
        cmd <- paste(cmd, paste("--", param$parameter, " ", param$value, sep = ""))
      }

      # Concatenate Nextflow and tool parameters
      # Add single-dash for nextflow parameters
      for (i in seq_len(nrow(self$nextflow_parameters))) {
        param <- self$nextflow_parameters[i, ]

        # Check if the parameter is not NULL and not FALSE
        if (!is.null(param$value) && param$value != FALSE) {
          # For flag parameters, only the parameter name should be added
          if (param$flag) {
            cmd <- paste(cmd, paste("-", param$parameter_name))
          } else {
            param_value_str <- if (is.character(param$value)) {
              shQuote(param$value)
            } else {
              as.character(param$value)
            }
            cmd <- paste(cmd, paste("-", param$parameter_name, " ", param_value_str))
          }
        }
      }

      message("Starting Nextflow pipeline...")
      message(cmd)
      system(cmd)
    },
    get_results = function() {
      # Define the root results directory
      results_dir <- file.path(self$run_path, "results")

      # Check if the results directory exists
      if (!dir.exists(results_dir)) {
        stop("No results found, you need to execute the run first!")
      }

      # Recursively list all files
      files <- list.files(path = results_dir, recursive = TRUE, full.names = TRUE)

      # Create data table with sample and tool information
      results_table <- data.table::rbindlist(lapply(files, function(file) {
        # Extract tool and sample name from the file path
        tool_sample <- strsplit(dirname(file), split = "/")[[1]][c(1,2)]
        data.table(sample = tool_sample[2], tool = tool_sample[1], path = file)
      }))

      return(results_table)
    },
    get_logs = function() {
      # Define a function that will extract the data from the Nextflow logs
      extract_nextflow_logs <- function(name) {
        command <- "nextflow"
        args <- c("log", name, "-f", "workdir,name")
        output <- system2(command, args, stdout = TRUE)

        # Split the output into lines
        lines <- strsplit(output, "\n")[[1]]

        # Create a data.frame from the lines
        parsed_output <- do.call(rbind, strsplit(lines, "\\s+", perl = TRUE))

        # Create the data.table
        log_table <- data.table(name = parsed_output[, 2], work_dir = parsed_output[, 1])

        return(log_table)
      }
      if (self$name) {
        log_table <- extract_nextflow_logs(self$name)
        return(log_table)
      } else {
        stop("No run found, you need to execute the run first!")
      }
    },
    clean = function(clean_params = "") {
      cmd <- paste("nextflow clean", clean_params)
      message("Cleaning up the work directory...")
      system(cmd)
    }
  )
)
