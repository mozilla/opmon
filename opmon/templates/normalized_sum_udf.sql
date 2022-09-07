CREATE TEMPORARY FUNCTION histogram_normalized_sum(
  arrs ARRAY<STRUCT<key INT64, value INT64>>,
  weight FLOAT64
)
RETURNS ARRAY<STRUCT<key INT64, value FLOAT64>> AS (
  -- Input: one histogram for a single client.
  -- Returns the normalized sum of the input maps.
  -- It returns the total_count[k] / SUM(total_count)
  -- for each key k.
  (
    WITH total_counts AS (
      SELECT
        sum(a.value) AS total_count
      FROM
        UNNEST(arrs) AS a
    ),
    summed_counts AS (
      SELECT
        a.key AS k,
        SUM(a.value) AS v
      FROM
        UNNEST(arrs) AS a
      GROUP BY
        a.key
    )
    SELECT
      ARRAY_AGG(
        STRUCT<key INT64, value FLOAT64>(
          k,
          COALESCE(SAFE_DIVIDE(1.0 * v, total_count), 0) * weight
        )
        ORDER BY
          SAFE_CAST(k AS INT64)
      )
    FROM
      summed_counts
    CROSS JOIN
      total_counts
  )
);
