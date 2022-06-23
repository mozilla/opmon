{{ header }}

CREATE OR REPLACE VIEW
  `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_alerts`
AS
WITH measured_values AS (
  -- get all scalar and histogram value for each day; group by metric and branch
  SELECT
    submission_date,
    probe,
    branch,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    STRUCT(
        mozfun.hist.merge(
          ARRAY_AGG(
            histogram IGNORE NULLS
          )
        ).values AS values
    ) AS values
  FROM `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_histogram`
  GROUP BY
    submission_date,
    probe,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    branch
  UNION ALL
  SELECT
    submission_date,
    probe,
    branch,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    STRUCT<values ARRAY<STRUCT<key FLOAT64, value FLOAT64>>>(mozfun.map.sum(
        ARRAY_AGG(
            STRUCT<key FLOAT64, value FLOAT64>(
                SAFE_CAST(COALESCE(value, 0.0) AS FLOAT64), 1
            )
        )
    )) AS values
  FROM `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_scalar`
  GROUP BY
    submission_date,
    probe,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    branch
),

ci_overlaps AS (
  -- check if confidence intervals between branches overlap  
  SELECT
    measured_values.submission_date,
    measured_values.probe,
    measured_values.branch,
    {% for dimension in dimensions -%}
        measured_values.{{ dimension.name }},
    {% endfor -%}
    (
        (
            udf_js.jackknife_percentile_ci(percentile.p, ref.values).high <= udf_js.jackknife_percentile_ci(percentile.p, measured_values.values).high 
            AND udf_js.jackknife_percentile_ci(percentile.p, measured_values.values).low <= udf_js.jackknife_percentile_ci(percentile.p, ref.values).high
        ) OR
        (
            udf_js.jackknife_percentile_ci(percentile.p, measured_values.values).low <= udf_js.jackknife_percentile_ci(percentile.p, ref.values).low 
            AND udf_js.jackknife_percentile_ci(percentile.p, ref.values).low <= udf_js.jackknife_percentile_ci(percentile.p, measured_values.values).high
        ) OR
        (
            udf_js.jackknife_percentile_ci(percentile.p, measured_values.values).high <= udf_js.jackknife_percentile_ci(percentile.p, ref.values).high
            AND udf_js.jackknife_percentile_ci(percentile.p, ref.values).low <= udf_js.jackknife_percentile_ci(percentile.p, measured_values.values).high
        ) OR
        (
            udf_js.jackknife_percentile_ci(percentile.p, ref.values).low <= udf_js.jackknife_percentile_ci(percentile.p, measured_values.values).low
            AND udf_js.jackknife_percentile_ci(percentile.p, measured_values.values).low <= udf_js.jackknife_percentile_ci(percentile.p, ref.values).high
        )
    ) AS ci_overlap,
    percentile.p AS percentile
  FROM measured_values, 
    (
        SELECT DISTINCT p 
        FROM UNNEST([
        {% for alert in alerts['ci_overlap'] %}
            {% for percentile in alert.percentiles %}
                {{ percentile }} {{ "," if not loop.last else "" }}
            {% endfor %}
            {{ "," if not loop.last else "" }}
        {% endfor %}
        ]) p
    ) AS percentile  -- percentiles to check
  LEFT JOIN measured_values AS ref
  ON 
    measured_values.submission_date = ref.submission_date AND
    measured_values.branch != ref.branch AND
    measured_values.probe = ref.probe
  WHERE 
    ref.branch = "{{ config.reference_branch }}"
    {% for alert in alerts['ci_overlap'] %}
        {{ "AND (" if loop.first else "" }}
        {% for percentile in alert.percentiles %}
            {% for probe in alert.probes %}
            (measured_values.probe = '{{ probe.name }}' AND percentile.p = {{ percentile }})
            {{ "OR" if not loop.last else "" }}
            {% endfor %}
        {{ "OR" if not loop.last else ")" }}
        {% endfor%}
    {% endfor %}
), hist_diffs AS (
    SELECT 
        measured_values.submission_date,
        measured_values.probe,
        measured_values.branch,
        {% for dimension in dimensions -%}
            measured_values.{{ dimension.name }},
        {% endfor -%}
        thresholds.percentile,
        ABS(
            AVG(udf_js.jackknife_percentile_ci(thresholds.percentile, measured_values.values).percentile) OVER (
                PARTITION BY 
                    branch, 
                    {% for dimension in dimensions -%}
                        {{ dimension.name }},
                    {% endfor -%}
                    measured_values.probe 
                ORDER BY submission_date ASC ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) -
            AVG(udf_js.jackknife_percentile_ci(thresholds.percentile, measured_values.values).percentile) OVER (
                PARTITION BY 
                    branch, 
                    {% for dimension in dimensions -%}
                        {{ dimension.name }},
                    {% endfor -%}
                    measured_values.probe 
                ORDER BY submission_date ASC ROWS BETWEEN 14 PRECEDING AND 7 PRECEDING)
        ) / AVG(udf_js.jackknife_percentile_ci(thresholds.percentile, measured_values.values).percentile) OVER (
                PARTITION BY 
                    branch, 
                    {% for dimension in dimensions -%}
                        {{ dimension.name }},
                    {% endfor -%}
                    measured_values.probe 
                ORDER BY submission_date ASC ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) > thresholds.max_relative_change AS diff
    FROM measured_values
    INNER JOIN
        UNNEST([
            STRUCT(
                "" AS probe,
                NULL AS percentile,
                NULL AS window_size,
                NULL AS max_relative_change
            )
            {{ "," if alerts['avg_diff']|length > 0 else "" }}
            {% for alert in alerts['avg_diff'] %}
                {% for percentile in alert.percentiles %}
                    {% for probe in alert.probes %}
                        STRUCT(
                            '{{ probe.name }}' AS probe,
                            {{ percentile }} AS percentile,
                            {{ alert.window_size }} AS window_size,
                            {{ alert.max_relative_change }} AS max_relative_change
                        )
                        {{ "," if not loop.last else "" }}
                    {% endfor%}
                    {{ "," if not loop.last else "" }}
                {% endfor %}
                {{ "," if not loop.last else "" }}
            {% endfor %}
        ]) thresholds
    ON
        thresholds.probe = measured_values.probe
)

-- checks for thresholds
SELECT 
    measured_values.submission_date,
    measured_values.probe,
    measured_values.branch,
    {% for dimension in dimensions -%}
        measured_values.{{ dimension.name }},
    {% endfor -%}
    thresholds.percentile,
    CASE
        WHEN udf_js.jackknife_percentile_ci(thresholds.percentile, values).low > thresholds.max THEN "Value above threshold"
        WHEN udf_js.jackknife_percentile_ci(thresholds.percentile, values).high < thresholds.min THEN "Value below threshold"
    END AS message
FROM measured_values
INNER JOIN
    UNNEST([
        STRUCT(
            "" AS probe,
            NULL AS percentile,
            NULL AS max,
            NULL AS min
        )
        {{ "," if alerts['threshold']|length > 0 else "" }}
        {% for alert in alerts['threshold'] %}
            {% for probe in alert.probes %}
                {% for percentile in alert.percentiles %}
                    STRUCT(
                        '{{ probe.name }}' AS probe,
                        {{ percentile }} AS percentile,
                        {% if alert.max == None %}
                            NULL AS max,
                        {% else %}
                            {{ alert.max[loop.index - 1] }} AS max,
                        {% endif %}
                        {% if alert.min == None %}
                            NULL AS min
                        {% else %}
                            {{ alert.min[loop.index - 1] }} AS min
                        {% endif %}
                    )
                    {{ "," if not loop.last else "" }}
                {% endfor%}
                {{ "," if not loop.last else "" }}
            {% endfor %}
            {{ "," if not loop.last else "" }}
        {% endfor %}
    ]) thresholds
ON
    measured_values.probe = thresholds.probe
WHERE
    (udf_js.jackknife_percentile_ci(thresholds.percentile, measured_values.values).low > thresholds.max OR
     udf_js.jackknife_percentile_ci(thresholds.percentile, measured_values.values).high < thresholds.min)

-- checks for differences in CI
UNION ALL
SELECT
  submission_date,
  probe,
  branch,
  {% for dimension in dimensions -%}
    {{ dimension.name }},
  {% endfor -%}
  percentile,
  "Significant difference between branches" AS message
FROM ci_overlaps
WHERE ci_overlap = FALSE

-- checks for significant changes
UNION ALL
SELECT 
    submission_date,
    probe,
    branch,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    percentile,
    "Significant difference to historical data" AS message
FROM hist_diffs
WHERE diff = TRUE AND submission_date > DATE_ADD(DATE('{{ config.start_date }}'), INTERVAL 7 DAY)
