from typing import Dict, Iterable, List, Mapping, Optional

import attr
from google.cloud import bigquery


@attr.s(auto_attribs=True, slots=True)
class BigQueryClient:
    project: str
    dataset: str
    _client: Optional[bigquery.client.Client] = None

    @property
    def client(self):
        self._client = self._client or bigquery.client.Client(self.project)
        return self._client

    def add_labels_to_table(self, table_name: str, labels: Mapping[str, str]) -> None:
        """Adds the provided labels to the table."""
        table_ref = self.client.dataset(self.dataset).table(table_name)
        table = self.client.get_table(table_ref)
        table.labels = labels

        self.client.update_table(table, ["labels"])

    def execute(
        self,
        query: str,
        destination_table: Optional[str] = None,
        write_disposition: Optional[bigquery.job.WriteDisposition] = None,
        clustering: Optional[List[str]] = None,
        time_partitioning: Optional[str] = None,
        partition_expiration_ms: Optional[int] = None,
    ) -> None:
        dataset = bigquery.dataset.DatasetReference.from_string(
            self.dataset,
            default_project=self.project,
        )
        kwargs = {
            "schema_update_options": bigquery.job.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            "allow_large_results": True,
            "use_query_cache": True,
        }
        if destination_table:
            kwargs["destination"] = dataset.table(destination_table)
            kwargs["write_disposition"] = bigquery.job.WriteDisposition.WRITE_TRUNCATE

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
        job.result(max_results=1)

    def load_table_from_json(
        self, results: Iterable[Dict], table: str, job_config: bigquery.LoadJobConfig
    ):
        # wait for the job to complete
        destination_table = f"{self.project}.{self.dataset}.{table}"
        self.client.load_table_from_json(results, destination_table, job_config=job_config).result()

    def delete_table(self, table_id: str) -> None:
        """Delete the table."""
        self.client.delete_table(table_id, not_found_ok=True)
