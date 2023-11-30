{{ header }}

{% include 'population.sql' %},

-- for each data source that is used
-- select the metric values
{% for data_source, metrics in metrics_per_dataset.items() -%}
merged_metrics_{{ data_source }} AS (
    SELECT
        DATE({{ metrics[0].data_source.submission_date_column }}) AS submission_date,
        CAST({{ metrics[0].data_source.client_id_column }} AS STRING) AS client_id,
        p.population_build_id AS build_id,
        {% for metric in metrics -%}
        {{ metric.select_expression }} AS {{ metric.name }},
        {% endfor -%}
    FROM
        {{ metrics[0].data_source.from_expr_for(app_id) }}
    RIGHT JOIN
        (
            SELECT
                client_id AS population_client_id,
                submission_date AS population_submission_date,
                build_id AS population_build_id,
            FROM
                population
            GROUP BY
                population_client_id,
                population_submission_date,
                population_build_id

        ) AS p
    ON
        {{ metrics[0].data_source.submission_date_column }} = p.population_submission_date
        {% if metrics[0].data_source.client_id_column != "NULL" %}
        AND {{ metrics[0].data_source.client_id_column }} = p.population_client_id
        {% endif %}
        {% if config.xaxis.value == "submission_date" %}
        AND p.population_build_id IS NULL
        {% else %}
        AND {{ metrics[0].data_source.build_id_column }} = p.population_build_id
        {% endif %}
    WHERE
        {% if config.xaxis.value == "submission_date" %}
        DATE({{ metrics[0].data_source.submission_date_column }}) = DATE('{{ submission_date }}')
        {% else %}
        -- when aggregating by build_id, only use the most recent 14 days of data
        DATE({{ metrics[0].data_source.submission_date_column }}) BETWEEN DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 14 DAY) AND DATE('{{ submission_date }}')
        {% endif %}
    GROUP BY
        submission_date,
        build_id,
        client_id
),
{% endfor %}

-- combine the metrics from all the data sources
joined_metrics AS (
  SELECT
    population.submission_date AS submission_date,
    population.client_id AS client_id,
    population.build_id,
    {% for dimension in dimensions -%}
      population.{{ dimension.name }} AS {{ dimension.name }},
    {% endfor %}
    population.branch AS branch,
    {% for data_source, metrics in metrics_per_dataset.items() -%}
        {% for metric in metrics -%}
            {{ metric.name }},
        {% endfor -%}
    {% endfor -%}
  FROM population
  {% for data_source, metrics in metrics_per_dataset.items() -%}
  LEFT JOIN merged_metrics_{{ data_source }}
  ON
    merged_metrics_{{ data_source }}.submission_date = population.submission_date AND
    {% if metrics[0].data_source.client_id_column != "NULL" %}
    merged_metrics_{{ data_source }}.client_id = population.client_id AND
    {% endif %}
    (population.build_id IS NULL OR merged_metrics_{{ data_source }}.build_id = population.build_id)
  {% endfor %}
),

-- normalize histograms and apply filters
normalized_metrics AS (
    SELECT
        *
    FROM joined_metrics
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
)
{% if first_run or config.xaxis.value == "submission_date" -%}
SELECT
    * REPLACE(DATE('{{ submission_date }}') AS submission_date)
FROM
    normalized_metrics
{% if config.xaxis.value == "build_id"%}
WHERE
    PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) >= DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 14 DAY)
{% endif %}
{% else -%}
-- if data is aggregated by build ID, then store metrics for most recent build_ids
SELECT
    * REPLACE(DATE('{{ submission_date }}') AS submission_date)
FROM normalized_metrics _current
WHERE
    PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) >= DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 14 DAY)
{% endif -%}
