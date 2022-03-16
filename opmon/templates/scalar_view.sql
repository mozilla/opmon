{{ header }}

CREATE OR REPLACE VIEW
  `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_scalar`
AS
WITH valid_builds AS (
    SELECT build_id
    FROM `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_scalar`
    WHERE {% include 'where_clause.sql' -%}
    GROUP BY 1
    -- todo adjust thresholds
    -- HAVING COUNT(DISTINCT client_id) >= {{ user_count_threshold }}
),

filtered_scalars AS (
    SELECT *
    FROM valid_builds
    INNER JOIN `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_scalar`
    USING (build_id)
    WHERE {% include 'where_clause.sql' -%}
)

SELECT
  client_id,
  {% if config.xaxis.value == "day" -%}
  submission_date,
  {% else %}
  build_id,
  {% endif %}
  {% for dimension in dimensions -%}
    {{ dimension.name }},
  {% endfor -%}
  branch,
  agg_type,
  name AS probe,
  CASE agg_type
    WHEN "MAX" THEN MAX(value)
    ELSE SUM(value)
  END AS value
FROM filtered_scalars
GROUP BY
  client_id,
  {% if config.xaxis.value == "day" -%}
  submission_date,
  {% else -%}
  build_id,
  {% endif -%}
  {% for dimension in dimensions -%}
    {{ dimension.name }},
  {% endfor -%}
  branch,
  agg_type,
  probe
