library(R6)
library(data.table)
library(digest) # for creating a hash
library(jsonlite)

#' Run R6Class
#'
#' An R6 object representing a Nextflow pipeline run. The class manages the execution
#' of the pipeline, as well as the retrieval of results and logs, and cleaning up 
#' after execution.
#'
#' @field pipeline_path The path to the pipeline script or project directory.
#' @field run_path The path to the directory where the pipeline results will be stored.
#' @field name An optional name for the run. Defaults to a digest of `run_path` if `NULL`.
#' @field nextflow_parameters A `data.table` holding parameters specific to Nextflow.
#' @field tool_parameters A `data.table` holding parameters specific to the pipeline tools.
#'
#' @export
Run <- R6Class("Run",
  public = list(
    pipeline_path = NULL,
    run_path = NULL,
    name = NULL,
    nextflow_parameters = data.table(),
    tool_parameters = data.table(),

    #' @description Initialize method of Run object
    #' @param pipeline_path path to the pipeline script or project directory
    #' @param run_path path to the directory where the pipeline results will be stored
    #' @param name an optional name for the run, defaults to a digest of run_path
    #' @export
    initialize = function(pipeline_path, run_path, name = NULL) {
      self$pipeline_path <- pipeline_path
      self$run_path <- run_path
      self$name <- ifelse(is.null(name), digest(run_path), name)

      # Create the data.table with the correct column order
      self$nextflow_parameters <- data.table(
        parameter_name = character(),
        value = character(),
        flag = logical(),
        desc = character()
      )

      # Populate the data.table with parameter data
      self$nextflow_parameters <- rbindlist(list(
        list(parameter_name = "-E", value = FALSE, flag = TRUE, desc = "Exports all current system environment"),
        list(parameter_name = "-ansi-log", value = NA_character_, flag = TRUE, desc = "Enable/disable ANSI console logging"),
        list(parameter_name = "-bucket-dir", value = NA_character_, flag = FALSE, desc = "Remote bucket where intermediate result files are stored"),
        list(parameter_name = "-cache", value = NA_character_, flag = TRUE, desc = "Enable/disable processes caching"),
        list(parameter_name = "-disable-jobs-cancellation", value = NA_character_, flag = TRUE, desc = "Prevent the cancellation of child jobs on execution termination"),
        list(parameter_name = "-dsl1", value = FALSE, flag = TRUE, desc = "Execute the workflow using DSL1 syntax"),
        list(parameter_name = "-dsl2", value = FALSE, flag = TRUE, desc = "Execute the workflow using DSL2 syntax"),
        list(parameter_name = "-dump-channels", value = NA_character_, flag = TRUE, desc = "Dump channels for debugging purpose"),
        list(parameter_name = "-dump-hashes", value = FALSE, flag = TRUE, desc = "Dump task hash keys for debugging purpose"),
        list(parameter_name = "-e.", value = NA_character_, flag = FALSE, desc = "Add the specified variable to execution environment"),
        list(parameter_name = "-entry", value = NA_character_, flag = FALSE, desc = "Entry workflow name to be executed"),
        list("-h, -help", FALSE, TRUE, "Print the command usage"),
        list(parameter_name = "-hub", value = NA_character_, flag = FALSE, desc = "Service hub where the project is hosted"),
        list(parameter_name = "-latest", value = FALSE, flag = TRUE, desc = "Pull latest changes before run"),
        list("-lib", NA_character_, FALSE, "Library extension path"),
        list(parameter_name = "-main-script", value = NA_character_, flag = FALSE, desc = "The script file to be executed when launching a project directory or repository"),
        list(parameter_name = "-offline", value = FALSE, flag = TRUE, desc = "Do not check for remote project updates"),
        list(parameter_name = "-params-file", value = NA_character_, flag = FALSE, desc = "Load script parameters from a JSON/YAML file"),
        list(parameter_name = "-plugins", value = NA_character_, flag = FALSE, desc = "Specify the plugins to be applied for this run e.g. nf-amazon,nf-tower"),
        list(parameter_name = "-preview", value = FALSE, flag = TRUE, desc = "Run the workflow script skipping the execution of all processes"),
        list(parameter_name = "-process.", value = NA_character_, flag = FALSE, desc = "Set process options"),
        list(parameter_name = "-profile", value = NA_character_, flag = FALSE, desc = "Choose a configuration profile"),
        list(parameter_name = "-queue-size", value = NA_character_, flag = FALSE, desc = "Max number of processes that can be executed in parallel by each executor"),
        list(parameter_name = "-resume", value = NA_character_, flag = TRUE, desc = "Execute the script using the cached results, useful to continue"),
        list(parameter_name = "-revision", value = NA_character_, flag = FALSE, desc = "Revision of the project to run (either a git branch, tag or commit SHA number)"),
        list(parameter_name = "-stub-run", value = FALSE, flag = TRUE, desc = "Execute the workflow replacing process scripts with command stubs"),
        list(parameter_name = "-test", value = NA_character_, flag = FALSE, desc = "Test a script function with the name specified"),
        list(parameter_name = "-user", value = NA_character_, flag = FALSE, desc = "Private repository user name"),
        list(parameter_name = "-with-apptainer", value = NA_character_, flag = TRUE, desc = "Enable process execution in a Apptainer container"),
        list(parameter_name = "-with-charliecloud", value = NA_character_, flag = TRUE, desc = "Enable process execution in a Charliecloud container runtime"),
        list(parameter_name = "-with-conda", value = NA_character_, flag = TRUE, desc = "Use the specified Conda environment package or file"),
        list(parameter_name = "-with-dag", value = NA_character_, flag = TRUE, desc = "Create pipeline DAG file"),
        list(parameter_name = "-with-docker", value = NA_character_, flag = TRUE, desc = "Enable process execution in a Docker container"),
        list(parameter_name = "-with-notification", value = NA_character_, flag = TRUE, desc = "Send a notification email on workflow completion to the specified recipients"),
        list(parameter_name = "-with-podman", value = NA_character_, flag = TRUE, desc = "Enable process execution in a Podman container"),
        list(parameter_name = "-with-singularity", value = NA_character_, flag = TRUE, desc = "Enable process execution in a Singularity container"),
        list(parameter_name = "-with-spack", value = NA_character_, flag = TRUE, desc = "Use the specified Spack environment package or file"),
        list(parameter_name = "-with-tower", value = NA_character_, flag = TRUE, desc = "Monitor workflow execution with Seqera Tower service"),
        list(parameter_name = "-with-weblog", value = NA_character_, flag = TRUE, desc = "Send workflow status messages via HTTP to target URL"),
        list(parameter_name = "-without-conda", value = NA_character_, flag = TRUE, desc = "Disable the use of Conda environments"),
        list(parameter_name = "-without-docker", value = FALSE, flag = TRUE, desc = "Disable process execution with Docker"),
        list(parameter_name = "-without-podman", value = NA_character_, flag = TRUE, desc = "Disable process execution in a Podman container"),
        list(parameter_name = "-without-spack", value = NA_character_, flag = TRUE, desc = "Disable the use of Spack environments"),
        list(parameter_name = "-work-dir", value = NA_character_, flag = FALSE, desc = "Directory where intermediate result files are stored")
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
        if ("definitions" %in% names(tool_parameters)) {
          for (definition_name in names(tool_parameters$definitions)) {
            definition <- tool_parameters$definitions[[definition_name]]
            if ("properties" %in% names(definition)) {
              properties <- definition$properties
              for (property_name in names(properties)) {
                property <- properties[[property_name]]
                if ("default" %in% names(property)) {
                  default_value <- property$default

                  if (property_name == "validate_params") {
                    next
                  }

                  type <- property$type # As an example
                  desc <- paste(property$description, property$help_text, sep = "\n")
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

    #' @description Execute method for the Nextflow pipeline
    #' @param samplesheet data.table or data.frame with samplesheet information to run the pipeline on
    #' @export
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
        empty_dt <- data.table(matrix(ncol = length(required_columns), dimnames = list(NULL, required_columns)))
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
      formatted_timestamp <- format(current_timestamp, "%Y-%m-%d_%H_%M_%S")
      print(formatted_timestamp)
      run_name <- paste0("run_", formatted_timestamp)
      self$name <- run_name

      cmd <- paste(
        "nextflow run",
        shQuote(self$pipeline_path),
        "-with-report",
        "-with-trace",
        "-with-timeline",
        paste("-name ", self$name),
        paste("--outdir", shQuote(self$run_path)),
        paste("--input", shQuote(samplesheet_path))
      )

      for (i in seq_len(nrow(self$tool_parameters))) {
        param <- self$tool_parameters[i, ]
        cmd <- paste(cmd, paste("--", param$parameter, " ", param$value, sep = ""))
      }

      for (i in seq_len(nrow(self$nextflow_parameters))) {
        param <- self$nextflow_parameters[i, ]

        # Check if the parameter is not NULL and not FALSE
        if (!is.na(param$value) && param$value != FALSE) {
          # For flag parameters, only the parameter name should be added
          if (param$flag) {
            cmd <- paste(cmd, paste("-", param$parameter_name))
          } else {
            param_value_str <- if (is.character(param$value)) {
              shQuote(param$value)
            } else {
              as.character(param$value)
            }
            cmd <- paste(cmd, paste(param$parameter_name, " ", param_value_str))
          }
        }
      }

      message("Starting Nextflow pipeline...")
      message(cmd)
      system(cmd)
    },


    #' @description Get the results of a pipeline run
    #' Retrieves results from the results directory after the pipeline has been executed.
    #' @return A data.table containing the paths to the results files.
    #' @export
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

    #' @description Get the logs of a Nextflow pipeline
    #' Extracts and returns the Nextflow logs of the pipeline run.
    #' @return A data.table containing extracted log information.
    #' @export
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
    #' @description Clean method for cleaning up the Nextflow work directory
    #' Executes the `nextflow clean` command with optional parameters.
    #' @param clean_params character string with additional parameters for the `nextflow clean` command
    #' @export
    clean = function(clean_params = "") {
      cmd <- paste("nextflow clean", clean_params)
      message("Cleaning up the work directory...")
      system(cmd)
    }
  )
)
