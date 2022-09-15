{{ header }}

CREATE OR REPLACE VIEW
  `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_alerts`
AS
SELECT 
    *
FROM
    `{{ gcp_project }}.{{ dataset }}_derived.{{ normalized_slug }}_alerts_v{{ table_version }}`
