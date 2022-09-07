{{ header }}

{% include 'normalized_sum_udf.sql' %}

WITH filtered_metrics AS (
    SELECT *
    FROM `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}`
    WHERE {% include 'where_clause.sql' -%}
),

-- bucket metrics that use percentile
buckets_by_metric AS (
  SELECT 
    [] AS dummy,
    {% set seen_metrics = [] %}
    {% for summary in summaries %}
        {% if summary.statistic.name == "percentile" %}
            {% if summary.metric.type == "scalar" -%}
                {% if summary.metric.name not in seen_metrics %}
                    {% if seen_metrics.append(summary.metric.name) %} {% endif %}
                    ARRAY(SELECT FORMAT("%.*f", 2, bucket) FROM UNNEST(
                        mozfun.glam.histogram_generate_scalar_buckets(
                            LOG(IF(MIN(value) <= 0, 1, MIN({{ summary.metric.name }})), 2), 
                            LOG(IF(MAX(value) <= 0, 1, MAX({{ summary.metric.name }})), 2), 
                            100
                        )
                    ) AS bucket ORDER BY bucket) AS {{ summary.metric.name }}_buckets,
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}
  FROM filtered_metrics
),


stats AS (
    SELECT
        {% if config.xaxis.value == "submission_date" -%}
        submission_date,
        {% else %}
        build_id,
        {% endif %}
        {% for dimension in dimensions -%}
            {{ dimension.name }},
        {% endfor -%}
        branch,
        ARRAY<STRUCT<
            metric STRING,
            statistic STRING,
            point FLOAT64,
            lower FLOAT64,
            upper FLOAT64,
            parameter STRING
        >>[
            {% for summary in summaries %}
                STRUCT(
                    '{{ summary.metric.name }}' AS metric,
                    '{{ summary.statistic.name }}' AS statistic,
                    {{ summary.statistic.point }} AS point
                    {% if summary.statistic.lower -%}
                    ,{{ summary.statistic.lower }} AS lower
                    {% endif -%}
                    {% if summary.statistic.upper -%}
                    ,{{ summary.statistic.upper }} AS upper
                    {% endif -%}
                    {% if summary.statistic.parameter -%}
                    ,'{{ summary.statistic.parameter }}' AS parameter
                    {% endif -%}
                )
                {{ "," if not loop.last else "" }}
            {% endfor %}
        ] AS statistics
    FROM
        `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}`
    CROSS JOIN buckets_by_metric
    WHERE submission_date = DATE("{{ submission_date }}")
    GROUP BY
        {% if config.xaxis.value == "submission_date" -%}
        submission_date,
        {% else %}
        build_id,
        {% endif %}
        {% for dimension in dimensions -%}
            {{ dimension.name }},
        {% endfor -%}
        branch
)

SELECT 
    {% if config.xaxis.value == "submission_date" -%}
    submission_date,
    {% else %}
    build_id,
    {% endif %}
    {% for dimension in dimensions -%}
        {{ dimension.name }},
    {% endfor -%}
    branch,
    statistic.metric AS metric,
    statistic.name AS statistic,
    statistic.point AS point,
    statistic.lower AS lower,
    statistic.upper AS upper,
    statistic.parameter AS parameter
FROM stats, UNNEST(statistics) as statistic
