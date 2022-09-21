{{ header }}

CREATE OR REPLACE VIEW
  `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_statistics`
AS

{% if config.xaxis.value == "submission_date" %}
WITH stats AS (
    SELECT 
        * 
    FROM
    `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_statistics_v{{ table_version }}`
)
{% else %}
WITH most_recent_date AS (
    SELECT 
        MAX(submission_date) AS most_recent
    FROM
        `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_statistics_v{{ table_version }}`
),
stats AS (
    SELECT
        *
    FROM 
        `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_statistics_v{{ table_version }}`,
        most_recent_date
    WHERE
        PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) = DATE_ADD(submission_date, INTERVAL 14 DAY) OR
        submission_date > DATE_SUB(most_recent, INTERVAL 14 DAY)
)
{% endif %}


{% for summary in summaries %}
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
        SAFE_CAST(statistic.parameter AS FLOAT64) AS parameter
    FROM 
        stats, 
        UNNEST({{ summary.metric.name }}_{{ summary.statistic.name() }}) AS statistic
    {{ "UNION ALL" if not loop.last else "" }}
{% endfor %}
