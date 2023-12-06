# NEXTFLOW.R 

An R wrapper around nextflow to execute nextflow pipelines without leaving an R
session.

## Requirements
- Run nextflow pipelines (e.g nf-jabba) from inside R
- Easily retrieve log files for a particular run
- Manage pipeline parameters and samplesheets
- Monitor pipeline execution (files output and logs)
* Incorporate AI

## Design
- `Run` R6 object

### Methods:

#### constructor
Inputs: 
    - pipeline_path: path to the pipeline repo/directory (e.g /nf-jabba)
    - run_path: path to the run directory (where the run will be executed, e.g ./)
    - name: human readable name for the run (optional, should default to the hash)
Outputs: Run object

#### execute()
executes the run (in background)

Inputs: samplesheet (as data.table)
Outputs: Void, executes the run

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
    - run_parameters: parameters specific to the run (probably just the hash)
