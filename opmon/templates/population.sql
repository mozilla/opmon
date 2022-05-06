WITH population AS (
    SELECT
        DATE({{ config.population.data_source.submission_date_column }}) AS submission_date,
        {{ config.population.data_source.client_id_column }} AS client_id,
        {{ config.population.data_source.build_id_column }} AS build_id,
        {% for dimension in dimensions -%}
          CAST({{ dimension.select_expression }} AS STRING) AS {{ dimension.name }},
        {% endfor -%}

        -- If a pref is defined, treat it as a rollout with an enabled and disabled branch.
        -- If branches are provided, use those instead.
        -- If neither a pref or branches are available, use the slug and treat it as a rollout
        -- where those with the slug have the feature enabled and those without do not.
        {% if config.population.branches|length > 0 -%}
        mozfun.map.get_key(
          environment.experiments,
          "{{ slug }}"
        ).branch AS branch,
        {% elif config.population.boolean_pref and config.population.branches is none -%}
        CASE
          WHEN SAFE_CAST({{ config.population.boolean_pref }} as BOOLEAN) THEN 'enabled'
          WHEN NOT SAFE_CAST({{ config.population.boolean_pref }} as BOOLEAN) THEN 'disabled'
        END
        AS branch,
        {% elif config.population.monitor_entire_population %}
          "active" AS branch,
        {% else -%}
          CASE WHEN
            mozfun.map.get_key(
              environment.experiments,
              "{{ slug }}"
            ).branch IS NULL THEN 'disabled'
          ELSE 'enabled'
          END AS branch,
        {% endif %}
    FROM
        {{ config.population.data_source.from_expression }}
    WHERE
        DATE({{ config.population.data_source.submission_date_column }}) = DATE('{{ submission_date }}')
        AND normalized_channel = '{{ config.population.channel.value }}'
    GROUP BY
        submission_date,
        client_id,
        build_id,
        {% for dimension in dimensions -%}
          {{ dimension.name }},
        {% endfor -%}
        branch
)