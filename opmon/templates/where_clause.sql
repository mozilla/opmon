{% if config.xaxis.value == "submission_date" %}
  {% if config.start_date %}
  DATE(submission_date) >= DATE("{{ config.start_date }}")
  {% else %}
  DATE(submission_date) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}
{% else %}
  {% if config.start_date %}
  PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) >= DATE("{{ config.start_date }}")
  {% else %}
  PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}
  AND DATE(submission_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
{% endif %}
