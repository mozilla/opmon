{{ header }}

CREATE OR REPLACE VIEW
  `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_statistics`
AS
{% if config.xaxis.value == "submission_date" %}
SELECT 
    *
FROM
    `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_statistics`
{% else %}
WITH most_recent_date AS (
    SELECT 
        MAX(submission_date) AS most_recent
    FROM
        `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_statistics`
)
SELECT
    *
FROM 
    `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_statistics`,
    most_recent_date
WHERE
    PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) = DATE_ADD(submission_date, INTERVAL 14 DAY) OR
    submission_date > DATE_SUB(most_recent, INTERVAL 14 DAY)
{% endif %}
