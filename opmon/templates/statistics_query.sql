{{ header }}

{% include 'normalized_sum_udf.sql' %}

{% include 'jackknife_percentile_ci_udf.sql' %}

WITH filtered_metrics AS (
    SELECT *
    FROM `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}`
    WHERE {% include 'where_clause.sql' -%}
),

-- bucket metrics that use percentile
log_min_max AS (
  SELECT
    NULL AS dummy,
    {% set seen_metrics = [] %}
    {% for summary in summaries %}
        {% if summary.statistic.name() == "percentile" %}
            {% if summary.metric.type == "scalar" -%}
                {% if summary.metric.name not in seen_metrics %}
                    {% if seen_metrics.append(summary.metric.name) %} {% endif %}
                    LOG(IF(MIN({{ summary.metric.name }}) <= 0, 1, MIN({{ summary.metric.name }})), 2) {{ summary.metric.name }}_log_min,
                    LOG(IF(MAX({{ summary.metric.name }}) <= 0, 1, MAX({{ summary.metric.name }})), 2) {{ summary.metric.name }}_log_max,
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}
  FROM
    filtered_metrics
),

buckets_by_metric AS (
  SELECT 
    NULL AS dummy,
    {% set seen_metrics = [] %}
    {% for summary in summaries %}
        {% if summary.statistic.name() == "percentile" %}
            {% if summary.metric.type == "scalar" -%}
                {% if summary.metric.name not in seen_metrics %}
                {% if seen_metrics.append(summary.metric.name) %} {% endif %}
                ARRAY(SELECT FORMAT("%.*f", 2, bucket) FROM UNNEST(
                    mozfun.glam.histogram_generate_scalar_buckets({{ summary.metric.name }}_log_min, {{ summary.metric.name }}_log_max, 100)
                ) AS bucket ORDER BY bucket) AS {{ summary.metric.name }}_buckets,
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}
  FROM log_min_max
),

-- todo: support custom dimensions
-- This generates an 'all' entry for each dimension, combining all values
-- with_all_dimensions AS (
--     SELECT
--         * 
--     FROM
--         `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}`
--     {% if dimensions | length > 0 %}
--     WHERE
--     {% for dimension in dimensions -%}
--         {{ dimension.name }} IS NOT NULL
--         {{ "AND" if not loop.last else "" }}
--     {% endfor -%}
--     {% endif %}
--     {% for perm in dimension_permutations %}
--     UNION ALL
--     SELECT
--         * REPLACE(
--             {% for is_all in perm %}
--                 {% if is_all %}
--                 NULL AS {{ dimensions[loop.index - 1].name }}
--                 {% else %}
--                 {{ dimensions[loop.index - 1].name }} AS {{ dimensions[loop.index - 1].name }}
--                 {% endif %}
--                 {{ "," if not loop.last else "" }}
--             {% endfor %}
--         )
--     FROM
--         `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}` AS o
--     WHERE
--         TRUE
--         {% for is_all in perm %}
--             {% if not is_all %}
--             AND o.{{ dimensions[loop.index - 1].name }} IS NOT NULL
--             {% endif %}
--         {% endfor %}
--     {% endfor %}
-- ),

stats AS (
    SELECT
        submission_date,
        build_id,
        {% for dimension in dimensions -%}
            {{ dimension.name }},
        {% endfor -%}
        branch,
        ARRAY_CONCAT(
            {% for summary in summaries %}
                {{ summary.statistic.compute(summary.metric) }}
                {{ "," if not loop.last else "" }}
            {% endfor %}
        ) AS statistics
    FROM
        `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}` 
    CROSS JOIN buckets_by_metric
    WHERE submission_date = DATE("{{ submission_date }}")
    GROUP BY
        submission_date,
        build_id,
        {% for dimension in dimensions -%}
            {{ dimension.name }},
        {% endfor -%}
        branch
)

SELECT 
    submission_date,
    build_id,
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    branch,
    statistic.metric AS metric,
    statistic.statistic AS statistic,
    statistic.point AS point,
    statistic.lower AS lower,
    statistic.upper AS upper,
    statistic.parameter AS parameter
FROM 
    stats, 
    UNNEST(statistics) AS statistic
