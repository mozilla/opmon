{{ header }}

CREATE OR REPLACE VIEW
  `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_scalar`
AS
WITH valid_builds AS (
    SELECT build_id
    FROM `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_scalar`
    WHERE {% include 'where_clause.sql' -%}
    GROUP BY 1
    -- todo adjust thresholds
    -- HAVING COUNT(DISTINCT client_id) >= {{ user_count_threshold }}
),

filtered_scalars AS (
    SELECT *
    FROM valid_builds
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
)

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
  SAFE_CAST(FORMAT("%.*f", 2, COALESCE(mozfun.glam.histogram_bucket_from_value(buckets, SAFE_CAST(value AS FLOAT64)), 0) + 0.0001) AS FLOAT64) AS value
FROM
    aggregated_scalars
  LEFT JOIN buckets_by_metric USING(name)

