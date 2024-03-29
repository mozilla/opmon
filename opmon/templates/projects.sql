{% if first_run -%}
CREATE TABLE `{{ gcp_project }}.{{ derived_dataset }}.{{ table }}` (
    slug STRING,
    name STRING,
    xaxis STRING,
    branches ARRAY<STRING>,
    dimensions ARRAY<STRING>,
    summaries ARRAY<STRUCT<statistic STRING, metric STRING, metric_groups ARRAY<STRING>>>,
    start_date DATE,
    end_date DATE,
    group_by_dimension STRING,
    alerting BOOLEAN,
    compact_visualization BOOLEAN,
);
{% endif -%}

BEGIN TRANSACTION;

DELETE FROM `{{ gcp_project }}.{{ derived_dataset }}.{{ table }}`
WHERE 
{% for project in projects %}
slug = "{{ project.slug }}"
{{ " OR " if not loop.last else "" }}
{% endfor %}
;

INSERT `{{ gcp_project }}.{{ derived_dataset }}.{{ table }}` 
(slug, name, xaxis, branches, dimensions, summaries, start_date, end_date, group_by_dimension, alerting, compact_visualization)
VALUES 
{% for project in projects %}
(
    "{{ project.slug }}", 
    "{{ project.config.name }}", 
    "{{ project.config.xaxis.value }}",
    [
        {% if project.config.population.branches|length > 0  -%}
            {% for branch in project.config.population.branches -%}
            "{{ branch }}"
            {{ "," if not loop.last else "" }}
            {% endfor -%}
        {% elif project.config.population.monitor_entire_population %}
            "active"
        {% else %}
            "enabled", "disabled"
        {% endif %}
    ],
    [
        {% for dimension in project.dimensions -%}
          "{{ dimension.name }}"
          {{ "," if not loop.last else "" }}
        {% endfor -%}
    ],
    [
        {% for summary in project.summaries -%}
        STRUCT(
            "{{ summary['statistic'] }}" AS statistic, 
            "{{ summary['metric'] }}" AS metric
            {% if summary['metric'] in project.metric_groups %}
            , CAST({{ project.metric_groups[summary['metric']] }} AS ARRAY<STRING>) AS metric_groups
            {% else %} 
            , CAST([] AS ARRAY<STRING>) as metric_groups
            {% endif %}
        )
        {{ "," if not loop.last else "" }}
        {% endfor %}
    ],
    DATE("{{ project.config.start_date }}"),
    {% if project.config.end_date -%}
    DATE("{{ project.config.end_date }}"),
    {% else -%}
    NULL,
    {% endif -%}
    {% if project.config.population.group_by_dimension -%}
    "{{ project.config.population.group_by_dimension.name }}"
    {% else -%}
    NULL
    {% endif -%},
    {% if project.alerts | length > 0 -%}
    TRUE
    {% else -%}
    FALSE
    {% endif -%},
    {{ project.config.compact_visualization }}
)
{{ "," if not loop.last else "" }}
{% endfor %}
;

COMMIT TRANSACTION;
