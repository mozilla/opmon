{{ header }}

{% include 'population.sql' %},

-- for each data source that is used
-- select the metric values
{% for data_source, metrics in metrics_per_dataset.items() -%}
merged_metrics_{{ data_source }} AS (
  SELECT
    DATE({{ metrics[0].data_source.submission_date_column }}) AS submission_date,
    {{ config.population.data_source.client_id_column }} AS client_id,
    p.population_build_id AS build_id,
    ARRAY<
      STRUCT<
        metric STRING,
        histograms ARRAY<
          STRUCT<
            bucket_count INT64,
            sum INT64,
            histogram_type INT64,
            `range` ARRAY<INT64>,
            values ARRAY<STRUCT<key INT64, value INT64>>>
        >>
    >[
      {% for metric in metrics %}
        (
            "{{ metric.name }}",
            {{ metric.select_expression }}
        )
        {{ "," if not loop.last else "" }}
      {% endfor %}
    ] AS metrics,
  FROM
    {{ metrics[0].data_source.from_expression }}
  RIGHT JOIN
    ( 
        SELECT
            client_id AS population_client_id,
            submission_date AS population_submission_date,
            build_id AS population_build_id
        FROM
            population
    ) AS p
  ON
    {{ metrics[0].data_source.submission_date_column }} = p.population_submission_date AND
    {{ config.population.data_source.client_id_column }} = p.population_client_id
  WHERE
      {% if config.xaxis.value == "submission_date" %}
      DATE({{ metrics[0].data_source.submission_date_column }}) = DATE('{{ submission_date }}')
      {% else %}
      -- when aggregating by build_id, only use the most recent 14 days of data
      DATE({{ metrics[0].data_source.submission_date_column }}) BETWEEN DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 14 DAY) AND DATE('{{ submission_date }}')
      {% endif %}
  GROUP BY
      submission_date,
      client_id,
      build_id
),
{% endfor %}

-- combine the metrics from all the data sources
joined_histograms AS (
  SELECT
    population.submission_date AS submission_date,
    population.client_id AS client_id,
    population.build_id,
    {% for dimension in dimensions %}
      population.{{ dimension.name }} AS {{ dimension.name }},
    {% endfor %}
    population.branch AS branch,
    {% if metrics_per_dataset != {} %}
    ARRAY_CONCAT(
      {% for data_source, metrics in metrics_per_dataset.items() %}
        merged_metrics_{{ data_source }}.metrics
      {% endfor %}
    ) AS metrics
    {% else %}
    [] AS metrics,
    {% endif %}
  FROM population
  {% for data_source, metrics in metrics_per_dataset.items() %}
  LEFT JOIN merged_metrics_{{ data_source }}
  USING(submission_date, client_id)
  {% endfor %}
),

-- merge histograms if client has multiple
merged_histograms AS (
  SELECT
    submission_date,
    client_id,
    build_id,
    branch,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
    {% if metrics_per_dataset != {} %}
    ARRAY_AGG(
      STRUCT<
        name STRING,
        histogram STRUCT<
          bucket_count INT64,
          sum INT64,
          histogram_type INT64,
          `range` ARRAY<INT64>,
          values ARRAY<STRUCT<key INT64, value INT64>>
        >
      > (
        metric,
        CASE
        WHEN
          histograms IS NULL
        THEN
          NULL
        ELSE
          mozfun.hist.merge(histograms)
        END
      )
    ) AS metrics
    {% else %}
    [] AS metrics
    {% endif %}
  FROM
    joined_histograms
  CROSS JOIN
    UNNEST(metrics)
  {% if not config.population.monitor_entire_population %}
  WHERE branch IN (
      -- If branches are not defined, assume it's a rollout
      -- and fall back to branches labeled as enabled/disabled
      {% if config.population.branches|length > 0  -%}
      {% for branch in config.population.branches -%}
        "{{ branch }}"
        {{ "," if not loop.last else "" }}
      {% endfor -%}
      {% else -%}
      "enabled", "disabled"
      {% endif -%}
  )
  {% endif %}
  GROUP BY
    submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
    branch
),

-- Cast histograms to have string keys so we can use the histogram normalization function
normalized_histograms AS (
  SELECT
      submission_date,
      client_id,
      build_id,
      {% for dimension in dimensions -%}
        {{ dimension.name }},
      {% endfor -%}
      branch,
      {% if metrics_per_dataset != {} %}
      name AS metric,
      {% else %}
      NULL AS metric,
      {% endif %}
      {% if metrics_per_dataset != {} %}
      STRUCT<
          bucket_count INT64,
          sum INT64,
          histogram_type INT64,
          `range` ARRAY<INT64>,
          VALUES
          ARRAY<STRUCT<key STRING, value INT64>>
      >(histogram.bucket_count,
          histogram.sum,
          histogram.histogram_type,
          histogram.range,
          ARRAY(SELECT AS STRUCT CAST(keyval.key AS STRING), keyval.value FROM UNNEST(histogram.values) keyval)
      ) AS value
      {% else %}
      NULL AS value
      {% endif %}
  FROM merged_histograms
  CROSS JOIN UNNEST(metrics)
)

{% if first_run or config.xaxis.value == "submission_date" -%}
SELECT
    submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor %}
    branch,
    metric AS probe,
    value
FROM 
  normalized_histograms
{% else -%}
SELECT
    DATE('{{ submission_date }}') AS submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor %}
    branch,
    metric AS probe,
    value
FROM normalized_histograms _current
WHERE 
    PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) >= DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 14 DAY)
UNION ALL
SELECT
    DATE('{{ submission_date }}') AS submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor %}
    branch,
    metric AS probe,
    value
FROM normalized_histograms _prev
WHERE 
    PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) < DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 14 DAY)
    AND submission_date = DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 1 DAY)
{% endif -%}
