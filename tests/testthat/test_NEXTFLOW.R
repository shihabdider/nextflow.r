devtools::load_all(".")

library(testthat)
library(nextflowr)
library(magrittr) # For piping `%>%`

# helper function to refresh the package after changes
refresh <- function() {
        devtools::load_all(".")

        pipeline_path <- "/Users/diders01/projects/jabba_prod/nf-jabba"
        run_path <- "/Users/diders01/projects/nextflowr/tests/testthat/nextflowr_test_run"

        run <- Run$new(pipeline_path, run_path, name = "test")
}

# A helper function to setup and create a new Run object
setup_run <- function() {
        devtools::load_all(".")
        pipeline_path <- "/Users/diders01/projects/jabba_prod/nf-jabba"
        run_path <- "/Users/diders01/projects/nextflowr/tests/testthat/nextflowr_test_run"

        Run$new(pipeline_path, run_path, name = "test_run")
}

test_that("Run$initialize sets parameters correctly", {
        refresh()
        run <- setup_run()

        expect_equal(run$pipeline_path, "/Users/diders01/projects/jabba_prod/nf-jabba")
        expect_equal(run$name, "test_run")
})

test_that("Run$execute writes samplesheet and generates command", {
        run <- setup_run()

        # Get pairs table
        samplesheet <- readRDS("../../inst/extdata/revison.ont.pairs.20230508_084910.rds")
        tumor_samples <- data.table(
                patient = samplesheet$pair,
                sample = paste0(samplesheet$pair, "_T"), # Append "_T" to the sample column values
                bam = samplesheet$tumor_bam
        )

        # Create the second data.table for normal samples
        normal_samples <- data.table(
                patient = samplesheet$pair,
                sample = paste0(samplesheet$pair, "_N"), # You might want to append "_N" to distinguish normal samples
                bam = samplesheet$normal_bam
        )

        # Append the second set of rows to the first data.table
        samplesheet_combined <- rbind(tumor_samples, normal_samples)

        # set step to sv_calling
        run$tool_parameters[run$tool_parameters$parameter == "step", value := "sv_calling"]
        run$nextflow_parameters[run$nextflow_parameters$parameter_name == "-profile", value := "docker"]

        run$execute(samplesheet_combined)

        expect_true(file.exists(file.path(run$run_path, "samplesheet.csv")))
})

test_that("Run$get_results retrieves correct results", {
        # Create a temporary directory to represent the run path
        run <- Run$new(pipeline_path = pipeline_path, run_path = run_path, name = "test_run_results")
        results_table <- run$get_results()
})

test_that("Run$get_logs retrieves logs correctly", {
        run <- Run$new(pipeline_path = pipeline_path, run_path = run_path, name = "test_run_logs")
        log_table <- run$get_logs()
})

test_that("Run$clean executes correctly", {
        run <- Run$new(pipeline_path = pipeline_path, run_path = run_path, name = "test_run_clean")
        run$clean()
})
