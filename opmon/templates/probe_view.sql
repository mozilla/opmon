{{ header }}

CREATE OR REPLACE VIEW
  `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}`
AS
-- Prepare scalar values
WITH valid_builds_scalar AS (
    SELECT build_id
    FROM `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_scalar`
    WHERE {% include 'where_clause.sql' -%}
    GROUP BY 1
    -- todo adjust thresholds
    -- HAVING COUNT(DISTINCT client_id) >= {{ user_count_threshold }}
),

filtered_scalars AS (
    SELECT *
    FROM valid_builds_scalar
    INNER JOIN `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_scalar`
    USING (build_id)
    WHERE {% include 'where_clause.sql' -%}
    {% if config.xaxis.value == "build_id" -%}
    AND DATE(submission_date) = (
      SELECT MAX(submission_date)
      FROM `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_scalar`
    )
    {% endif -%}
),

log_min_max AS (
  SELECT
    name,
    LOG(IF(MIN(value) <= 0, 1, MIN(value)), 2) log_min,
    LOG(IF(MAX(value) <= 0, 1, MAX(value)), 2) log_max
  FROM
    filtered_scalars
  GROUP BY name),

buckets_by_metric AS (
  SELECT 
    name, 
    ARRAY(SELECT FORMAT("%.*f", 2, bucket) FROM UNNEST(
      mozfun.glam.histogram_generate_scalar_buckets(log_min, log_max, 100)
    ) AS bucket ORDER BY bucket) AS buckets
  FROM log_min_max
),

aggregated_scalars AS (
  SELECT
    client_id,
    {% if config.xaxis.value == "submission_date" -%}
    submission_date,
    {% else %}
    build_id,
    {% endif %}
    {% for dimension in dimensions -%}
      {{ dimension.name }},
    {% endfor -%}
    branch,
    agg_type,
    name,
    CASE
      agg_type
    WHEN
      "MAX"
    THEN
      MAX(value)
    ELSE
      SUM(value)
    END
    AS value
  FROM
    filtered_scalars
  GROUP BY
    client_id,
    {% if config.xaxis.value == "submission_date" -%}
    submission_date,
    {% else -%}
    build_id,
    {% endif -%}
    {% for dimension in dimensions -%}
      {{ dimension.name }},
    {% endfor -%}
    branch,
    agg_type,
    name
),

-- Prepare histogram values
valid_builds_histograms AS (
    SELECT build_id
    FROM `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_histogram`
    WHERE {% include 'where_clause.sql' -%}
    GROUP BY 1
    -- todo adjust thresholds
    -- HAVING COUNT(DISTINCT client_id) >= {{ user_count_threshold }}
),

filtered_histograms AS (
    SELECT *
    FROM valid_builds_histograms
    INNER JOIN `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_histogram`
    USING (build_id)
    WHERE {% include 'where_clause.sql' -%}
    {% if config.xaxis.value == "build_id" -%}
    AND DATE(submission_date) = (
      SELECT MAX(submission_date)
      FROM `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_histogram`
    )
    {% endif -%}
),

normalized_histograms AS (
    SELECT
        client_id,
        {% if config.xaxis.value == "submission_date" -%}
        submission_date,
        {% else -%}
        build_id,
        {% endif -%}
        {% for dimension in dimensions -%}
          {{ dimension.name }},
        {% endfor -%}
        branch,
        probe,
        {% if probes_per_dataset != {} %}
        STRUCT<
            bucket_count INT64,
            sum INT64,
            histogram_type INT64,
            `range` ARRAY<INT64>,
            VALUES
            ARRAY<STRUCT<key STRING, value FLOAT64>>
        >(
            ANY_VALUE(value.bucket_count),
            ANY_VALUE(value.sum),
            ANY_VALUE(value.histogram_type),
            ANY_VALUE(value.range),
            mozfun.glam.histogram_normalized_sum(
                mozfun.hist.merge(ARRAY_AGG(value IGNORE NULLS)).values,
                1.0
            )
        ) AS value
        {% else %}
        NULL AS value
        {% endif %}
        FROM filtered_histograms
        GROUP BY
        client_id,
        {% if config.xaxis.value == "submission_date" -%}
        submission_date,
        {% else -%}
        build_id,
        {% endif %}
        {% for dimension in dimensions -%}
          {{ dimension.name }},
        {% endfor -%}
        branch,
        probe)

-- Cast histograms to have FLOAT64 keys
-- so we can use the histogram jackknife percentile function.
SELECT
    client_id,
    {% if config.xaxis.value == "submission_date" -%}
    submission_date,
    {% else -%}
    build_id,
    {% endif %}
    {% for dimension in dimensions -%}
      {{ dimension.name }},
    {% endfor -%}
    branch,
    "SUM" AS agg_type,
    probe,
    {% if probes_per_dataset != {} %}
    STRUCT<
        bucket_count INT64,
        sum INT64,
        histogram_type INT64,
        `range` ARRAY<INT64>,
        VALUES
        ARRAY<STRUCT<key FLOAT64, value FLOAT64>
    >>(value.bucket_count,
        value.sum,
        value.histogram_type,
        value.range,
        ARRAY(SELECT AS STRUCT CAST(keyval.key AS FLOAT64), keyval.value FROM UNNEST(value.values) keyval)
    ) AS value
    {% else %}
    NULL AS value
    {% endif %}
FROM normalized_histograms
UNION ALL
SELECT
  client_id,
  {% if config.xaxis.value == "submission_date" -%}
  submission_date,
  {% else %}
  build_id,
  {% endif %}
  {% for dimension in dimensions -%}
    {{ dimension.name }},
  {% endfor -%}
  branch,
  agg_type,
  name AS probe,
  STRUCT<
      bucket_count INT64,
      sum INT64,
      histogram_type INT64,
      `range` ARRAY<INT64>,
      VALUES
      ARRAY<STRUCT<key FLOAT64, value FLOAT64>
  >>(1,
      COALESCE(SAFE_CAST(SAFE_CAST(FORMAT("%.*f", 2, COALESCE(mozfun.glam.histogram_bucket_from_value(buckets, SAFE_CAST(value AS FLOAT64)), 0) + 0.0001) AS FLOAT64) AS INT64), 0),
      1,
      [0, COALESCE(SAFE_CAST(SAFE_CAST(FORMAT("%.*f", 2, COALESCE(mozfun.glam.histogram_bucket_from_value(buckets, SAFE_CAST(value AS FLOAT64)), 0) + 0.0001) AS FLOAT64) AS INT64), 0)],
      [STRUCT<key FLOAT64, value FLOAT64>(
        COALESCE(SAFE_CAST(FORMAT("%.*f", 2, COALESCE(mozfun.glam.histogram_bucket_from_value(buckets, SAFE_CAST(value AS FLOAT64)), 0) + 0.0001) AS FLOAT64), 0.0), 1
      )]
  ) AS value
FROM
    aggregated_scalars
  LEFT JOIN buckets_by_metric USING(name)
