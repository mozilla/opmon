{{ header }}

CREATE OR REPLACE VIEW
  `{{ gcp_project }}.{{ dataset }}.{{ normalized_slug }}_alerts`
AS
SELECT 
    *
FROM
    `{{ gcp_project }}.{{ derived_dataset }}.{{ normalized_slug }}_alerts_v{{ table_version }}`
