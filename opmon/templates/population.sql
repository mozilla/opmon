WITH population AS (
    SELECT
        DATE({{ config.population.data_source.submission_date_column }}) AS submission_date,
        CAST({{ config.population.data_source.client_id_column }} AS STRING) AS client_id,
        {% if config.xaxis.value == "submission_date" %}
        NULL AS build_id,
        {% else %}
        {{ config.population.data_source.build_id_column }} AS build_id,
        {% endif %}
        {% for dimension in dimensions -%}
          CAST({{ dimension.select_expression }} AS STRING) AS {{ dimension.name }},
        {% endfor -%}

        -- If a pref is defined, treat it as a rollout with an enabled and disabled branch.
        -- If branches are provided, use those instead.
        -- If neither a pref or branches are available, use the slug and treat it as a rollout
        -- where those with the slug have the feature enabled and those without do not.
        {% if config.population.branches|length > 0 -%}
        mozfun.map.get_key(
          {% if is_glean_app -%}
          ping_info.experiments,
          {% else -%}
          environment.experiments,
          {% endif -%}
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
              {% if is_glean_app -%}
              ping_info.experiments,
              {% else -%}
              environment.experiments,
              {% endif -%}
              "{{ slug }}"
            ).branch IS NULL THEN 'disabled'
          ELSE 'enabled'
          END AS branch,
        {% endif %}
    FROM
        {%- if config.population.data_source %}
        (
          -- filter out clients that send more than 10000 pings
          SELECT * 
          FROM {{ config.population.data_source.from_expr_for(app_id) }} 
          WHERE {{ config.population.data_source.client_id_column }} NOT IN (
            SELECT {{ config.population.data_source.client_id_column }} AS client_id
            FROM {{ config.population.data_source.from_expr_for(app_id) }} 
            WHERE {{ config.population.data_source.submission_date_column }} = DATE('{{ submission_date }}')
            -- client_id is set to NULL when we want to compute metrics across all clients (default is per client)
            -- keep 'NULL' clients
            AND {{ config.population.data_source.client_id_column }} IS NOT NULL
            GROUP BY client_id
            HAVING COUNT(*) > 10000
          )
        )
        {%- else %}
        None
        {%- endif %}
    WHERE
        {% if config.xaxis.value == "submission_date" %}
        DATE({{ config.population.data_source.submission_date_column }}) = DATE('{{ submission_date }}')
        {% else %}
        -- when aggregating by build_id, only use the most recent 14 days of data
        DATE({{ config.population.data_source.submission_date_column }}) BETWEEN DATE_SUB(DATE('{{ submission_date }}'), INTERVAL 14 DAY) AND DATE('{{ submission_date }}')
        {% endif %}
        {% if config.population.channel %}
        AND normalized_channel = '{{ config.population.channel.value }}'
        {% endif %}
    GROUP BY
        submission_date,
        client_id,
        build_id,
        {% for dimension in dimensions -%}
          {{ dimension.name }},
        {% endfor -%}
        branch
)