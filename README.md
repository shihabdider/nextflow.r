# NEXTFLOW.R 

An R wrapper around nextflow to execute nextflow pipelines without leaving an R
session.

## Design
- `Run` R6 object

### Methods:

#### constructor
Inputs: 

    - pipeline_path: path to the pipeline repo/directory (e.g /nf-jabba)
    - run_path: path to the run directory (where the run will be executed, e.g ./)
    - outdir: path to the output directory (if different from default `$run_path/results`)
    - name: human readable name for the run (optional, will default to a hash or datestamp)

Outputs: Run object

#### execute()
executes the run

Inputs: samplesheet (as data.table)
Outputs: Void, executes the run

N.B if starting from a pairs table, you will need to subset the table to
retrive the columns necessary for the samplesheet for the step you wish to
start the pipeline from. Running execute with missing required columns in the
samplesheet, will return an empty samplesheet with all required columns.

To adjust parameters for a particular run, simply modify the
`$nextflow_parameters` (for nextflow specific parameters) or `tool_parameters`
(for parameters specific to a tool in the pipeline) data.tables.

e.g

```

pipeline_path <- "/Users/diders01/projects/jabba_prod/nf-jabba"
run_path <- "/Users/diders01/projects/nextflowr/tests/testthat"

run <- Run$new(pipeline_path, run_path, name = "test")
# set the starting step to sv_calling (default is alignment)
run$tool_parameters$input_output_options$step$value = "sv_calling"
# set profile to docker (default is NA)
run$nextflow_parameters[run$nextflow_parameters$parameter_name == "-profile", value := "docker"]
```
#### get_logs()
retrieves paths to logs for the run

Inputs:

    - fields: a vector of field names to retrieve from logs (`c('name', 'workdir', 'status')` by default)
    - list_all_fields: a boolean; if set returns all field names

Outputs: data.table containing log data (columns correspond to fields)

#### get_results()
retrieves paths to output files for the run

Inputs: None
Outputs: data.table containing paths to all output files for all tools (columns are: sample, tool, filename, path)

#### clean()
clean up work directory

Inputs: None
Outputs: Void, runs `nextflow clean` to remove cached files
