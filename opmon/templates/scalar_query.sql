{{ header }}

{% include 'population.sql' %},

{% for data_source, probes in probes_per_dataset.items() -%}
merged_scalars_{{ data_source }} AS (
    SELECT
        DATE({{ config.population.data_source.submission_date_column }}) AS submission_date,
        {{ config.population.data_source.client_id_column }} AS client_id,
        ARRAY<
            STRUCT<
                name STRING,
                agg_type STRING,
                value INT64
            >
        >[
          {% for probe in probes -%}
            (
                "{{ probe.name }}",
                "MAX",
                MAX(CAST({{ probe.select_expression }} AS INT64))
            ),
            (
                "{{ probe.name }}",
                "SUM",
                SUM(CAST({{ probe.select_expression }} AS INT64))
            )
            {{ "," if not loop.last else "" }}
          {% endfor -%}
        ] AS metrics,
    FROM
        `{{ probes[0].data_source.from_expression }}`
    WHERE
        DATE({{ config.population.data_source.submission_date_column }}) = DATE('{{ submission_date }}')
    GROUP BY
        submission_date,
        client_id
),
{% endfor %}
joined_scalars AS (
  SELECT
    population.submission_date AS submission_date,
    population.client_id AS client_id,
    population.build_id,
    {% for dimension in dimensions -%}
      population.{{ dimension.name }} AS {{ dimension.name }},
    {% endfor %}
    population.branch AS branch,
    ARRAY_CONCAT(
      {% for data_source, probes in probes_per_dataset.items() -%}
        merged_scalars_{{ data_source }}.metrics
      {% endfor -%}
    ) AS metrics
  FROM population
  {% for data_source, probes in probes_per_dataset.items() -%}
  LEFT JOIN merged_scalars_{{ data_source }}
  USING(submission_date, client_id)
  {% endfor %}
),
flattened_scalars AS (
    SELECT *
    FROM joined_scalars
    CROSS JOIN UNNEST(metrics)
    {% if config.population.branches != [] or config.population.boolean_pref %}
    WHERE branch IN (
        -- If branches are not defined, assume it's a rollout
        -- and fall back to branches labeled as enabled/disabled
        {% if config.population.branches != [] -%}
        {% for branch in config.population.branches -%}
          "{{ branch }}"
          {{ "," if not loop.last else "" }}
        {% endfor -%}
        {% elif config.population.boolean_pref -%}
        "enabled", "disabled"
        {% endif -%}
    )
    {% endif -%}
)
{% if first_run or str(config.xaxis) == "submission_date" -%}
SELECT
    submission_date,
    client_id,
    build_id,
    {% for dimension in dimensions -%}
      {{ dimension.name }},
    {% endfor -%}
    branch,
    name,
    agg_type,
    SAFE_CAST(value AS FLOAT64) AS value
FROM
    flattened_scalars
{% else -%}
-- if data is aggregated by build ID, then aggregate data with previous runs
SELECT
    '{{ submission_date }}' AS submission_date,
    IF(_current.client_id IS NOT NULL, _current, _prev).* REPLACE (
      IF(_current.agg_type IS NOT NULL,
        CASE _current.agg_type
          WHEN "SUM" THEN SUM(SAFE_CAST(_current.value AS FLOAT64), _prev.value)
          WHEN "MAX" THEN MAX(SAFE_CAST(_current.value AS FLOAT64), _prev.value)
          ELSE SAFE_CAST(_current.value AS FLOAT64)
        END,
        CASE _prev.agg_type
          WHEN "SUM" THEN SUM(SAFE_CAST(_prev.value AS FLOAT64), _prev.value)
          WHEN "MAX" THEN MAX(SAFE_CAST(_prev.value AS FLOAT64), _prev.value)
          ELSE SAFE_CAST(_prev.value AS FLOAT64)
        END
      ) AS value
    )
FROM
    flattened_scalars _current
FULL JOIN
    `{{ gcp_project }}.{{ dataset }}.{{ slug }}_scalar` _prev
ON 
  DATE_SUB(_prev.submission_date, INTERVAL 1 DAY) = _current.submission_date AND
  _prev.client_id = _current.client_id AND
  _prev.build_id = _current.build_id AND
  {% for dimension in dimensions %}
      _prev.{{ dimension.name }} = _current.{{ dimension.name }} AND
  {% endfor %}
  _prev.branch = _current.branch AND
  _prev.name = _current.name AND
  _prev.agg_type = _current.agg_type
WHERE _prev.submission_date = DATE_SUB('{{ submission_date }}', INTERVAL 1 DAY)
{% endif -%}
