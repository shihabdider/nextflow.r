devtools::load_all(".")

pipeline_path <- "/Users/diders01/projects/jabba_prod/nf-jabba"
run_path <- "."

run <- Run$new(pipeline_path, run_path, name = "test")

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

refresh <- function() {
        devtools::load_all(".")

        pipeline_path <- "/Users/diders01/projects/jabba_prod/nf-jabba"
        run_path <- "."

        run <- Run$new(pipeline_path, run_path, name = "test")
}
refresh()
run$execute(samplesheet_combined)

library(jsonlite)
schema_path <- "/Users/diders01/projects/jabba_prod/nf-jabba/nextflow_schema.json"
if (file.exists(schema_path)) {
        tool_parameters <- jsonlite::fromJSON(schema_path, simplifyVector = FALSE)
} else {
        stop("nextflow_schema.json not found in pipeline_path")
}
