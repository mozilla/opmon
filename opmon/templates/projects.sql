{% if first_run -%}
CREATE TABLE `{{ gcp_project }}.{{ dataset }}_derived.{{ table }}` (
    slug STRING,
    name STRING,
    xaxis STRING,
    branches ARRAY<STRING>,
    dimensions ARRAY<STRING>,
    probes ARRAY<STRUCT<agg_type STRING, name STRING>>,
    start_date DATE,
    end_date DATE,
    group_by_dimension STRING,
);
{% endif -%}

DELETE FROM `{{ gcp_project }}.{{ dataset }}_derived.{{ table }}`
WHERE slug = "{{ slug }}";

INSERT `{{ gcp_project }}.{{ dataset }}_derived.{{ table }}` 
(slug, name, xaxis, branches, dimensions, probes, start_date, end_date, group_by_dimension)
VALUES (
    "{{ slug }}", 
    "{{ config.name }}", 
    "{{ config.xaxis.value }}",
    [
        {% if config.population.monitor_entire_population %}
            "active"
        {% else %}
            {% if config.population.branches|length > 0  -%}
            {% for branch in config.population.branches -%}
            "{{ branch }}"
            {{ "," if not loop.last else "" }}
            {% endfor -%}
            {% else -%}
            "enabled", "disabled"
            {% endif -%}
        {% endif %}
    ],
    [
        {% for dimension in dimensions -%}
          "{{ dimension.name }}"
          {{ "," if not loop.last else "" }}
        {% endfor -%}
    ],
    [
        {% for probe in probes -%}
        STRUCT("{{ probe.agg_type }}" AS agg_type, "{{ probe.name }}" AS name)
        {{ "," if not loop.last else "" }}
        {% endfor %}
    ],
    DATE("{{ config.start_date }}"),
    {% if config.end_date -%}
    DATE("{{ config.end_date }}"),
    {% else -%}
    NULL,
    {% endif -%}
    {% if config.population.group_by_dimension -%}
    "{{ config.population.group_by_dimension }}"
    {% else -%}
    NULL
    {% endif -%}
);
