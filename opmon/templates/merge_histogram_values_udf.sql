CREATE TEMPORARY FUNCTION merge_histogram_values(
  arrs ANY TYPE
) AS (
  STRUCT(
    ARRAY(
      SELECT AS STRUCT
        key,
        SUM(value) AS value
      FROM
        UNNEST(arrs) AS histogram
      GROUP BY
        key
      ORDER BY
        key
    ) AS values
  )
);