# dlt-san


## Deploying the pipelines
```
// Deploy a pipeline
databricks pipelines deploy pipeline-flujos.json

// Get status
databricks pipelines get --pipeline-id 98217f51-52d7-4cc2-afec-2847f06f1764

// Run a pipeline
databricks pipelines run --pipeline-id 98217f51-52d7-4cc2-afec-2847f06f1764

// Stop a pipeline
databricks pipelines stop --pipeline-id 98217f51-52d7-4cc2-afec-2847f06f1764

// Delete a pipeline
databricks pipelines delete --pipeline-id 98217f51-52d7-4cc2-afec-2847f06f1764
```
