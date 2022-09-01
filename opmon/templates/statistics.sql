WITH merged AS (
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
        probe AS metric,
        mozfun.hist.merge(ARRAY_AGG(value IGNORE NULLS)).values AS values
    FROM
        `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}`
    WHERE submission_date = DATE("{{ submission_date }}")
    GROUP BY
        {% if config.xaxis.value == "submission_date" -%}
        submission_date,
        {% else %}
        {% for dimension in dimensions -%}
            {{ dimension.name }},
        {% endfor -%}
        build_id,
        {% endif %}
        branch,
        metric
), stats AS (
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
        metric,
        CASE value
            {% for probe in probes %}
            WHEN probe = "{{ probe.metric.name }}" 
            THEN ARRAY<STRUCT<>>[(
                {% for stat in probe.statistics %}
                {{ stat.name }} AS statistic,
                {{ stat.point }} AS point,
                {% if stat.lower -%}
                stat.lower AS lower,
                {% endif -%}
                {% if stat.upper -%}
                stat.upper AS upper,
                {% endif -%}
                {% if stat.parameter -%}
                stat.parameter AS parameter,
                {% endif -%}
                {% enfor %}
            )]
            {% endfor %}
            ELSE NULL
        END AS values
    FROM
        merged
    GROUP BY
        {% if config.xaxis.value == "submission_date" -%}
        submission_date,
        {% else %}
        {% for dimension in dimensions -%}
            {{ dimension.name }},
        {% endfor -%}
        build_id,
        {% endif %}
        branch,
        metric
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
    metric,
    statistic.name AS statistic,
    statistic.point AS point,
    statistic.lower AS lower,
    statistic.upper AS upper,
    statistic.parameter AS parameter
FROM stats, UNNEST(values) as statistic
