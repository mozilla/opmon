"""BigQuery handler."""

from typing import Any, Dict, Iterable, List, Optional, Protocol, Union

import attr
from google.cloud import bigquery


class BeforeExecuteCallback(Protocol):
    """Optional callback invoked before each `execute`."""

    def __call__(
        self,
        query: str,
        job_config: Optional[bigquery.job.QueryJobConfig],
        annotations: Dict[str, Any] = {},
    ) -> None:
        """Invoke before each `execute`.

        Parameters are the BigQuery SQL string, the BigQuery job configuration,
        and an optional dict of consumer-provided annotations for, e.g.,
        including a relevant date, query type, etc.
        """
        pass


def sql_table_id(table):
    """Get the standard sql format fully qualified id for a table."""
    return f"{table.project}.{table.dataset_id}.{table.table_id}"


@attr.s(auto_attribs=True, slots=True)
class BigQueryClient:
    """Handler for requests to BigQuery."""

    project: str
    dataset: str
    _client: Optional[bigquery.client.Client] = None

    before_execute_callback: Optional[BeforeExecuteCallback] = None

    @property
    def client(self) -> bigquery.client.Client:
        """Return BigQuery client instance."""
        self._client = self._client or bigquery.client.Client(self.project)
        return self._client

    def execute(
        self,
        query: Union[str, List[str]],
        destination_table: Optional[str] = None,
        write_disposition: Optional[bigquery.job.WriteDisposition] = None,
        clustering: Optional[List[str]] = None,
        time_partitioning: Optional[str] = None,
        partition_expiration_ms: Optional[int] = None,
        dataset: Optional[str] = None,
        join_keys: Optional[List[str]] = None,
        annotations: Dict[str, Any] = {},
    ) -> None:
        """Execute a SQL query and applies the provided parameters."""
        bq_dataset = bigquery.dataset.DatasetReference.from_string(
            dataset if dataset else self.dataset,
            default_project=self.project,
        )

        kwargs: Dict[str, Any] = {
            "allow_large_results": True,
            "use_query_cache": False,
        }
        base_kwargs = kwargs.copy()

        if destination_table:
            kwargs["destination"] = bq_dataset.table(destination_table)
            kwargs["write_disposition"] = bigquery.job.WriteDisposition.WRITE_APPEND
            kwargs["schema_update_options"] = bigquery.job.SchemaUpdateOption.ALLOW_FIELD_ADDITION

        if write_disposition:
            kwargs["write_disposition"] = write_disposition

        if clustering is not None:
            kwargs["clustering_fields"] = clustering

        if time_partitioning:
            if partition_expiration_ms:
                kwargs["time_partitioning"] = bigquery.TimePartitioning(
                    field=time_partitioning, expiration_ms=partition_expiration_ms
                )
            else:
                kwargs["time_partitioning"] = bigquery.TimePartitioning(field=time_partitioning)

        parts = []
        if isinstance(query, list):
            if not join_keys:
                raise ValueError("multipart query specified without join keys")

            for idx, part in enumerate(query):
                config = bigquery.job.QueryJobConfig(default_dataset=bq_dataset, **base_kwargs)

                if callable(self.before_execute_callback):
                    annotations["part"] = f"part-{idx}"
                    self.before_execute_callback(part, config, annotations)

                job = self.client.query(part, config)
                # block on result
                job.result()
                parts.append(job)

            # redefine query as a join over the parts, so that things like destination
            # table and schema update options are available for the result
            query = (
                "SELECT\n  _0.*,\n"
                + "".join(
                    f"  _{i}.* EXCEPT({', '.join(join_keys)}),\n"
                    for i, _ in enumerate(parts)
                    if i > 0
                )
                + f"FROM\n  `{sql_table_id(parts[0].destination)}` AS _0\n"
                + "".join(
                    f"JOIN\n  `{sql_table_id(job.destination)}` AS _{i}\n"
                    + "ON\n  "
                    + "   AND ".join(
                        "(\n"
                        f"    _0.{join_key} = _{i}.{join_key}\n"
                        f"    OR (_0.{join_key} IS NULL AND _{i}.{join_key} IS NULL)\n"
                        "  )\n"
                        for join_key in join_keys
                    )
                    for i, job in enumerate(parts)
                    if i > 0
                )
            )

        try:
            config = bigquery.job.QueryJobConfig(default_dataset=bq_dataset, **kwargs)

            if callable(self.before_execute_callback):
                if len(parts) > 0:
                    annotations["part"] = "joined"
                self.before_execute_callback(query, config, annotations)

            job = self.client.query(query, config)
            # block on result
            job.result()
        finally:
            for job in parts:
                self.client.delete_table(job.destination)

    def load_table_from_json(
        self, results: Iterable[Dict], table: str, job_config: bigquery.LoadJobConfig
    ) -> None:
        """Write the provided dictionary to the provided table."""
        # wait for the job to complete
        destination_table = f"{self.project}.{self.dataset}.{table}"
        self.client.load_table_from_json(results, destination_table, job_config=job_config).result()
