{% if str(config.xaxis) == "submission_date" %}
  {% if config.start_date %}
  DATE(submission_date) >= "{{ config.start_date.strftime('%Y-%m-%d') }}"
  {% else %}
  DATE(submission_date) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}
{% else %}
  {% if config.start_date %}
  PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) >= "{{ config.start_date.strftime('%Y-%m-%d') }}"
  {% else %}
  PARSE_DATE('%Y%m%d', CAST(build_id AS STRING)) > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}
  AND DATE(submission_date) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
{% endif %}
