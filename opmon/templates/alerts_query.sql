{{ header }}

WITH measured_values AS (
  -- get all scalar and histogram value for each day; group by metric and branch
  SELECT
    submission_date,
    build_id,
    branch,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    metric,
    statistic,
    point,
    lower,
    upper,
    parameter
  FROM `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_statistics`
),

ci_overlaps AS (
  -- check if confidence intervals between branches overlap  
  SELECT
    measured_values.submission_date,
    measured_values.build_id,
    measured_values.metric,
    measured_values.statistic,
    measured_values.branch,
    {% for dimension in dimensions -%}
        measured_values.{{ dimension.name }},
    {% endfor -%}
    (
        (
            ref.upper <= measured_values.upper
            AND measured_values.lower <= ref.upper
        ) OR
        (
            measured_values.lower <= ref.lower
            AND ref.lower<= measured_values.upper
        ) OR
        (
            measured_values.upper <= ref.upper
            AND ref.lower<= measured_values.upper
        ) OR
        (
            ref.lower<= measured_values.lower
            AND measured_values.lower <= ref.upper
        )
    ) AS ci_overlap,
    measured_values.parameter AS parameter
  FROM measured_values
  LEFT JOIN measured_values AS ref
  ON 
    measured_values.submission_date = ref.submission_date AND
    measured_values.branch != ref.branch AND
    measured_values.metric = ref.metric AND
    measured_values.statistic = ref.statistic AND
    ((measured_values.build_id IS NULL AND ref.build_id IS NULL) OR measured_values.build_id = ref.build_id)
  WHERE 
    measured_values.submission_date = DATE('{{ submission_date }}') AND
    ref.branch = "{{ config.reference_branch }}"
    {% for alert in alerts['ci_overlap'] %}
        {{ "AND (" if loop.first else "" }}
        {% for metric in alert.metrics %}
            (
                measured_values.metric = '{{ metric.metric.name }}' 
                {% if alerts.statistics %}
                AND (measured_values.statistic IN {{ alerts.statistics }})
                {% endif %}
                {% if alert.parameters %}
                {% for param in alert.parameters %}
                AND (measured_values.parameter IS NULL OR measured_values.parameter = "{{ param }}")
                {% endfor %}
                {% endif %}
            )
            {{ "OR" if not loop.last else ")" }}
        {% endfor %}
    {% endfor %}
), 
{% for hist_diff_alert in alerts['avg_diff'] %}
hist_diffs_{{ hist_diff_alert.name }} AS (
    SELECT 
        measured_values.submission_date,
        measured_values.build_id,
        measured_values.metric,
        measured_values.statistic,
        measured_values.branch,
        {% for dimension in dimensions -%}
            measured_values.{{ dimension.name }},
        {% endfor -%}
        measured_values.parameter AS parameter,
        {{ hist_diff_alert.window_size }} AS window_size,
        SAFE_DIVIDE(ABS(
            AVG(measured_values.point) OVER (
                PARTITION BY 
                    branch, 
                    {% for dimension in dimensions -%}
                        {{ dimension.name }},
                    {% endfor -%}
                    measured_values.metric,
                    measured_values.statistic,
                    SAFE_CAST(measured_values.parameter AS STRING)
                ORDER BY 
                    {% if config.xaxis.value == "submission_date" %}
                    submission_date 
                    {% else %}
                    build_id
                    {% endif %}
                ASC ROWS BETWEEN {{ hist_diff_alert.window_size - 1 }} PRECEDING AND CURRENT ROW) -
            AVG(measured_values.point) OVER (
                PARTITION BY 
                    branch, 
                    {% for dimension in dimensions -%}
                        {{ dimension.name }},
                    {% endfor -%}
                    measured_values.metric,
                    measured_values.statistic,
                    SAFE_CAST(measured_values.parameter AS STRING)
                ORDER BY 
                    {% if config.xaxis.value == "submission_date" %}
                    submission_date 
                    {% else %}
                    build_id
                    {% endif %}
                ASC ROWS BETWEEN {{ hist_diff_alert.window_size - 1 + hist_diff_alert.window_size - 1 }} PRECEDING AND {{ hist_diff_alert.window_size - 1 }} PRECEDING)
        ), AVG(measured_values.point) OVER (
                PARTITION BY 
                    branch, 
                    {% for dimension in dimensions -%}
                        {{ dimension.name }},
                    {% endfor -%}
                    measured_values.metric,
                    measured_values.statistic,
                    SAFE_CAST(measured_values.parameter AS STRING)
                ORDER BY 
                    {% if config.xaxis.value == "submission_date" %}
                    submission_date 
                    {% else %}
                    build_id
                    {% endif %}
                ASC ROWS BETWEEN {{ hist_diff_alert.window_size - 1 }} PRECEDING AND CURRENT ROW)
        ) > {{ hist_diff_alert.max_relative_change }} AS diff
    FROM measured_values
    WHERE measured_values.metric IN (
        {% for metric in hist_diff_alert.metrics %}
        '{{ metric.metric.name }}'
        {{ "," if not loop.last else "" }}
        {% endfor %}
    )
    {% if hist_diff_alert.statistics %}
    AND (measured_values.statistic IN {{ alerts.statistics }})
    {% endif %}
    {% if hist_diff_alert.parameters %}
    {% for param in hist_diff_alert.parameters %}
    AND (measured_values.parameter IS NULL OR measured_values.parameter = "{{ param }}")
    {% endfor %}
    {% endif %}
),
{% endfor %}
hist_diffs AS (
    {% if alerts['avg_diff']| length > 0 %}
    {% for hist_diff_alert in alerts['avg_diff'] %}
    SELECT 
        *
    FROM 
        hist_diffs_{{ hist_diff_alert.name }}
    {% if config.xaxis.value == "submission_date" %}
    WHERE submission_date = DATE("{{ submission_date }}")
    {% else %}
    WHERE PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) = DATE("{{ submission_date }}")
    {% endif %}
    {{ "UNION ALL" if not loop.last else "" }}
    {% endfor %}
    {% else %}
    SELECT
        NULL AS submission_date,
        NULL AS build_id,
        NULL AS metric,
        NULL AS statistic,
        NULL AS branch,
        {% for dimension in dimensions -%}
            NULL AS {{ dimension.name }},
        {% endfor -%}
        NULL AS parameter,
        NULL AS diff,
        NULL AS window_size,
    {% endif %}
)

-- checks for thresholds
SELECT 
    DATE("{{ submission_date }}") AS submission_date,
    measured_values.build_id,
    measured_values.metric,
    measured_values.statistic,
    measured_values.branch,
    {% for dimension in dimensions -%}
        measured_values.{{ dimension.name }},
    {% endfor -%}
    SAFE_CAST(thresholds.parameter AS FLOAT64) AS parameter,
    CASE
        WHEN COALESCE(measured_values.lower, measured_values.point) > thresholds.max THEN "Value above threshold"
        WHEN COALESCE(measured_values.upper, measured_values.point) < thresholds.min THEN "Value below threshold"
    END AS message
FROM measured_values
INNER JOIN
    UNNEST(
        ARRAY<STRUCT<
            metric STRING,
            statistic STRING,
            parameter STRING,
            max FLOAT64,
            min FLOAT64
        >>[
        STRUCT(
            "" AS metric,
            "" AS statistic,
            NULL AS parameter,
            NULL AS max,
            NULL AS min
        )
        {{ "," if alerts['threshold']|length > 0 else "" }}
        {% for alert in alerts['threshold'] %}
            {% for metric in alert.metrics %}
                {% if alert.parameters %}
                {% for parameter in alert.parameters %}
                    {% for statistic in (alert.statistics if alert.statistics else [None]) %}
                    STRUCT(
                        '{{ metric.metric.name }}' AS metric
                        {% if statistic %}
                        , '{{ statistic }}' AS statistic
                        {% else %}
                        , NULL AS statistic
                        {% endif %}
                        , '{{ parameter }}' AS parameter
                        {% if alert.max %}
                            , {{ alert.max[loop.index - 1] }} AS max
                        {% else %}
                            , NULL AS max
                        {% endif %}
                        {% if alert.min %}
                            , {{ alert.min[loop.index - 1] }} AS min
                        {% else %}
                            , NULL AS min
                        {% endif %}
                    )
                    {{ "," if not loop.last else "" }}
                    {% endfor %}
                    {{ "," if not loop.last else "" }}
                {% endfor%}
                {% else %}
                    {% for statistic in (alert.statistics if alert.statistics else [None]) %}
                    STRUCT(
                        '{{ metric.metric.name }}' AS metric
                        {% if statistic %}
                        , '{{ statistic }}' AS statistic
                        {% else %}
                        , NULL AS statistic
                        {% endif %}
                        , NULL AS parameter
                        {% if alert.max %}
                            , {{ alert.max[0] }} AS max
                        {% else %}
                            , NULL AS max
                        {% endif %}
                        {% if alert.min %}
                            , {{ alert.min[0] }} AS min
                        {% else %}
                            , NULL AS min
                        {% endif %}
                    )
                    {{ "," if not loop.last else "" }}
                    {% endfor %}
                {% endif %}
                {{ "," if not loop.last else "" }}
            {% endfor %}
            {{ "," if not loop.last else "" }}
        {% endfor %}
    ]) AS thresholds
ON
    measured_values.metric = thresholds.metric AND
    (thresholds.statistic IS NULL OR measured_values.statistic = thresholds.statistic) 
WHERE
    measured_values.submission_date = DATE('{{ submission_date }}') AND
    (COALESCE(measured_values.lower, measured_values.point) > thresholds.max AND 
     (thresholds.parameter IS NULL OR SAFE_CAST(thresholds.parameter AS FLOAT64) = SAFE_CAST(measured_values.parameter AS FLOAT64)) OR
     COALESCE(measured_values.upper, measured_values.point) < thresholds.min)

-- checks for differences in CI
UNION ALL
SELECT
  DATE("{{ submission_date }}") AS submission_date,
  build_id,
  metric,
  statistic,
  branch,
  {% for dimension in dimensions -%}
    {{ dimension.name }},
  {% endfor -%}
  SAFE_CAST(parameter AS FLOAT64) AS parameter,
  "Significant difference between branches" AS message
FROM ci_overlaps
WHERE ci_overlap = FALSE

-- checks for significant changes
UNION ALL
SELECT DISTINCT
    DATE("{{ submission_date }}") AS submission_date,
    build_id,
    metric,
    statistic,
    branch,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    SAFE_CAST(parameter AS FLOAT64) AS parameter,
    "Significant difference to historical data" AS message
FROM hist_diffs
WHERE diff = TRUE AND submission_date > DATE_ADD(DATE('{{ config.start_date }}'), INTERVAL window_size DAY)
