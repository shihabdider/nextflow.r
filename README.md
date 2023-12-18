# NEXTFLOW.R 

An R wrapper around nextflow to execute nextflow pipelines without leaving an R
session.

## Requirements
- Run nextflow pipelines (e.g nf-jabba) from inside R
- Easily retrieve log files for a particular run
- Manage pipeline parameters and samplesheets
- Monitor pipeline execution (files output and logs)
* Incorporate AI (planned)

## Design
- `Run` R6 object

### Methods:

#### constructor
Inputs: 
    - pipeline_path: path to the pipeline repo/directory (e.g /nf-jabba)
    - run_path: path to the run directory (where the run will be executed, e.g ./)
    - name: human readable name for the run (optional, should default to a hash)
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
run$tool_parameters[run$tool_parameters$parameter == "step", value := "sv_calling"]
# set profile to docker (default is NA)
run$nextflow_parameters[run$nextflow_parameters$parameter_name == "-profile", value := "docker"]
```


#### get_logs()
retrieves paths to logs for the run

Inputs: None, it should be able to run via its attributes
Outputs: data.table containing paths to all log files and output files

#### check_status()
reports the current output

Inputs: None, it should be able to run via its attributes
Outputs: prints the contents of the bg monitor file

#### clean()
clean up work directory

Inputs: `nextflow clean` cli parameters (as string?)
Outputs: Void, executes the run

### Attributes:
    - nextflow_parameters: data.table containing all parameters specific to
      nextflow
    - tool_parameters: data.table with parameters for each tool in the run
    - name: name of the run
