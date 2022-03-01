from typing import Dict, Iterable, Mapping, Optional

import attr
import google.cloud.bigquery
import google.cloud.bigquery.client
import google.cloud.bigquery.dataset
import google.cloud.bigquery.job
import google.cloud.bigquery.table


@attr.s(auto_attribs=True, slots=True)
class BigQueryClient:
    project: str
    dataset: str
    _client: Optional[google.cloud.bigquery.client.Client] = None

    @property
    def client(self):
        self._client = self._client or google.cloud.bigquery.client.Client(self.project)
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
        write_disposition: Optional[google.cloud.bigquery.job.WriteDisposition] = None,
    ) -> None:
        dataset = google.cloud.bigquery.dataset.DatasetReference.from_string(
            self.dataset,
            default_project=self.project,
        )
        kwargs = {}
        if destination_table:
            kwargs["destination"] = dataset.table(destination_table)
            kwargs["write_disposition"] = google.cloud.bigquery.job.WriteDisposition.WRITE_TRUNCATE

        if write_disposition:
            kwargs["write_disposition"] = write_disposition

        config = google.cloud.bigquery.job.QueryJobConfig(default_dataset=dataset, **kwargs)
        job = self.client.query(query, config)
        # block on result
        job.result(max_results=1)

    def load_table_from_json(
        self, results: Iterable[Dict], table: str, job_config: google.cloud.bigquery.LoadJobConfig
    ):
        # wait for the job to complete
        destination_table = f"{self.project}.{self.dataset}.{table}"
        self.client.load_table_from_json(results, destination_table, job_config=job_config).result()

    def delete_table(self, table_id: str) -> None:
        """Delete the table."""
        self.client.delete_table(table_id, not_found_ok=True)
