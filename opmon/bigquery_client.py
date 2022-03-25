"""BigQuery handler."""
from typing import Dict, Iterable, List, Optional

import attr
from google.cloud import bigquery


@attr.s(auto_attribs=True, slots=True)
class BigQueryClient:
    """Handler for requests to BigQuery."""

    project: str
    dataset: str
    _client: Optional[bigquery.client.Client] = None

    @property
    def client(self) -> bigquery.client.Client:
        """Return BigQuery client instance."""
        self._client = self._client or bigquery.client.Client(self.project)
        return self._client

    def execute(
        self,
        query: str,
        destination_table: Optional[str] = None,
        write_disposition: Optional[bigquery.job.WriteDisposition] = None,
        clustering: Optional[List[str]] = None,
        time_partitioning: Optional[str] = None,
        partition_expiration_ms: Optional[int] = None,
        dataset: Optional[str] = None,
    ) -> None:
        """Execute a SQL query and applies the provided parameters."""
        dataset = bigquery.dataset.DatasetReference.from_string(
            dataset if dataset else self.dataset,
            default_project=self.project,
        )

        kwargs = {
            "allow_large_results": True,
            "use_query_cache": False,
        }

        if destination_table:
            kwargs["destination"] = dataset.table(destination_table)
            kwargs["write_disposition"] = bigquery.job.WriteDisposition.WRITE_TRUNCATE
            kwargs["schema_update_options"] = bigquery.job.SchemaUpdateOption.ALLOW_FIELD_ADDITION

        if write_disposition:
            kwargs["write_disposition"] = write_disposition

        if clustering:
            kwargs["clustering_fields"] = clustering

        if time_partitioning:
            if partition_expiration_ms:
                kwargs["time_partitioning"] = bigquery.TimePartitioning(
                    field=time_partitioning, expiration_ms=partition_expiration_ms
                )
            else:
                kwargs["time_partitioning"] = bigquery.TimePartitioning(field=time_partitioning)

        config = bigquery.job.QueryJobConfig(default_dataset=dataset, **kwargs)
        job = self.client.query(query, config)
        # block on result
        job.result()

    def load_table_from_json(
        self, results: Iterable[Dict], table: str, job_config: bigquery.LoadJobConfig
    ) -> None:
        """Write the provided dictionary to the provided table."""
        # wait for the job to complete
        destination_table = f"{self.project}.{self.dataset}.{table}"
        self.client.load_table_from_json(results, destination_table, job_config=job_config).result()
