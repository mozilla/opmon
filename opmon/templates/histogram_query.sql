{{ header }}

{% include 'population.sql' %},

{% for data_source, probes in probes_per_dataset.items() -%}
merged_probes_{{ data_source }} AS (
  SELECT
    DATE({{ probes[0].data_source.submission_date_column }}) AS submission_date,
    {{ config.population.data_source.client_id_column }} AS client_id,
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
      {% for probe in probes %}
        (
            "{{ probe.name }}",
            ARRAY_AGG(mozfun.hist.extract({{ probe.select_expression }}) IGNORE NULLS)
        )
        {{ "," if not loop.last else "" }}
      {% endfor %}
    ] AS metrics,
  FROM
    {{ probes[0].data_source.from_expression }}
  WHERE
        {{ probes[0].data_source.submission_date_column }} = DATE('{{ submission_date }}')
    GROUP BY
        submission_date,
        client_id
),
{% endfor %}
joined_histograms AS (
  SELECT
    population.submission_date AS submission_date,
    population.client_id AS client_id,
    population.build_id,
    {% for dimension in dimensions %}
      population.{{ dimension.name }} AS {{ dimension.name }},
    {% endfor %}
    population.branch AS branch,
    ARRAY_CONCAT(
      {% for data_source, probes in probes_per_dataset.items() %}
        merged_probes_{{ data_source }}.metrics
      {% endfor %}
    ) AS metrics
  FROM population
  {% for data_source, probes in probes_per_dataset.items() %}
  LEFT JOIN merged_probes_{{ data_source }}
  USING(submission_date, client_id)
  {% endfor %}
),
merged_histograms AS (
  SELECT
    submission_date,
    client_id,
    build_id,
    branch,
    {% for dimension in dimensions %}
      {{ dimension.name }},
    {% endfor %}
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
normalized_histograms AS (
  -- Cast histograms to have string keys so we can use the histogram normalization function
  SELECT
      submission_date,
      client_id,
      build_id,
      {% for dimension in dimensions -%}
        {{ dimension.name }},
      {% endfor -%}
      branch,
      name AS probe,
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
  FROM merged_histograms
  CROSS JOIN UNNEST(metrics)
)
{% if first_run or config.xaxis.value == "submission_date" -%}
SELECT
  * 
FROM 
normalized_histograms
{% else -%}
SELECT
    IF(_current.client_id IS NOT NULL, _current, _prev).* REPLACE (
      DATE('{{ submission_date }}') AS submission_date,
      IF(_current.value IS NOT NULL,
        IF(_prev.value IS NOT NULL, mozfun.hist.merge([_current.value, _prev.value]), _current.value),
        _prev.value
      ) AS value
    )
FROM
    normalized_histograms _current
FULL JOIN (
  SELECT * FROM
    `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_histogram`
  WHERE submission_date = DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 1 DAY)
) AS _prev
ON 
  DATE_SUB(_prev.submission_date, INTERVAL 1 DAY) = _current.submission_date AND
  _prev.client_id = _current.client_id AND
  _prev.build_id = _current.build_id AND
  {% for dimension in dimensions -%}
      _prev.{{ dimension.name }} = _current.{{ dimension.name }} AND
  {% endfor -%}
  _prev.branch = _current.branch 
{% endif %}


