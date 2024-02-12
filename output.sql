-- Generated via opmon


WITH population AS (
    SELECT
        DATE(submission_date) AS submission_date,
        CAST(client_id AS STRING) AS client_id,
        
        NULL AS build_id,
        
        CAST(app AS STRING) AS app,
        -- If a pref is defined, treat it as a rollout with an enabled and disabled branch.
        -- If branches are provided, use those instead.
        -- If neither a pref or branches are available, use the slug and treat it as a rollout
        -- where those with the slug have the feature enabled and those without do not.
        
          "active" AS branch,
        
    FROM
        (
          -- filter out clients that send more than 10000 pings
          SELECT * 
          FROM     (
        SELECT
            "vpnsession" as source_table,
            DATE(v.submission_timestamp) AS submission_date,
            CONCAT(client_info.os, " - vpn") as app,
            CASE v.normalized_app_id
                WHEN "mozillavpn" THEN v.client_info.client_id
                ELSE v.metrics.uuid.session_installation_id
            END AS client_id,
            v.metrics.uuid.session_session_id AS session_id,
            v.metrics.datetime.session_session_start AS session_start,
            v.metrics.datetime.session_session_end AS session_end,
        FROM `moz-fx-data-shared-prod.mozilla_vpn.vpnsession` AS v

        UNION ALL
        SELECT 
            "daemonsession" AS source_table,
            DATE(d.submission_timestamp) AS submission_date,
            CONCAT(client_info.os, " - daemon") AS app,
            d.metrics.uuid.session_installation_id AS client_id,
            d.metrics.uuid.session_daemon_session_id AS session_id,
            d.metrics.datetime.session_daemon_session_start AS session_start,
            d.metrics.datetime.session_daemon_session_end AS session_end,
        FROM `moz-fx-data-shared-prod.mozilla_vpn.daemonsession` AS d
    )
 
          WHERE client_id NOT IN (
            SELECT client_id AS client_id
            FROM     (
        SELECT
            "vpnsession" as source_table,
            DATE(v.submission_timestamp) AS submission_date,
            CONCAT(client_info.os, " - vpn") as app,
            CASE v.normalized_app_id
                WHEN "mozillavpn" THEN v.client_info.client_id
                ELSE v.metrics.uuid.session_installation_id
            END AS client_id,
            v.metrics.uuid.session_session_id AS session_id,
            v.metrics.datetime.session_session_start AS session_start,
            v.metrics.datetime.session_session_end AS session_end,
        FROM `moz-fx-data-shared-prod.mozilla_vpn.vpnsession` AS v

        UNION ALL
        SELECT 
            "daemonsession" AS source_table,
            DATE(d.submission_timestamp) AS submission_date,
            CONCAT(client_info.os, " - daemon") AS app,
            d.metrics.uuid.session_installation_id AS client_id,
            d.metrics.uuid.session_daemon_session_id AS session_id,
            d.metrics.datetime.session_daemon_session_start AS session_start,
            d.metrics.datetime.session_daemon_session_end AS session_end,
        FROM `moz-fx-data-shared-prod.mozilla_vpn.daemonsession` AS d
    )
 
            WHERE submission_date = DATE('2024-02-09')
            -- client_id is set to NULL when we want to compute metrics across all clients (default is per client)
            -- keep 'NULL' clients
            AND client_id IS NOT NULL
            GROUP BY client_id
            HAVING COUNT(*) > 10000
          )
        )
    WHERE
        
        DATE(submission_date) = DATE('2024-02-09')
        
        
    GROUP BY
        submission_date,
        client_id,
        build_id,
        app,
        branch
),

-- for each data source that is used
-- select the metric values
merged_metrics_session_duration_table AS (
    SELECT
        DATE(submission_date) AS submission_date,
        CAST(client_id AS STRING) AS client_id,
        p.population_build_id AS build_id,
        app AS app,
        AVG(session_duration) AS avg_session_duration,
        SUM(session_duration) AS avg_sum_session_duration,
        FROM
            (
        SELECT 
            submission_date,
            app,
            client_id,
            session_id,
            TIMESTAMP_DIFF(MAX(session_end), MIN(session_start), MINUTE) AS session_duration,
            COUNT(session_start) AS cnt_session_starts,
            COUNT(session_end) as cnt_session_ends

        FROM (
            SELECT
                DATE(v.submission_timestamp) AS submission_date,
                CONCAT(client_info.os, " - vpn") as app,
                CASE v.normalized_app_id
                    WHEN "mozillavpn" then v.client_info.client_id
                    ELSE v.metrics.uuid.session_installation_id
                END AS client_id,
                v.metrics.uuid.session_session_id AS session_id,
                v.metrics.datetime.session_session_start as session_start,
                v.metrics.datetime.session_session_end as session_end,
            FROM `moz-fx-data-shared-prod.mozilla_vpn.vpnsession` v

            UNION ALL
            SELECT 
                DATE(d.submission_timestamp) AS submission_date,
                CONCAT(client_info.os, " - daemon") AS app,
                d.metrics.uuid.session_installation_id AS client_id,
                d.metrics.uuid.session_daemon_session_id as session_id,
                d.metrics.datetime.session_daemon_session_start as session_start,
                d.metrics.datetime.session_daemon_session_end as session_end,
            FROM `moz-fx-data-shared-prod.mozilla_vpn.daemonsession` d
        )

        GROUP BY 
            submission_date,
            app,
            client_id,
            session_id
    )

    RIGHT JOIN
        (
            SELECT
                client_id AS population_client_id,
                submission_date AS population_submission_date,
                build_id AS population_build_id,
                app AS population_app,
                FROM
                population
            GROUP BY
                population_submission_date,
                population_client_id,
                population_build_id
                ,population_app
                ) AS p
    ON
        submission_date = p.population_submission_date
        
        AND client_id = p.population_client_id
        
        
        AND p.population_build_id IS NULL
        
        AND app = p.population_app
        
    WHERE
        
        DATE(submission_date) = DATE('2024-02-09')
        
    GROUP BY
        submission_date,
        build_id,
        client_id
        ,app
        
),
merged_metrics_base_table AS (
    SELECT
        DATE(submission_date) AS submission_date,
        CAST(client_id AS STRING) AS client_id,
        p.population_build_id AS build_id,
        app AS app,
        COUNT(DISTINCT client_id) AS active_subscribers,
        COUNT(DISTINCT session_id) AS session_count,
        FROM
            (
        SELECT
            "vpnsession" as source_table,
            DATE(v.submission_timestamp) AS submission_date,
            CONCAT(client_info.os, " - vpn") as app,
            CASE v.normalized_app_id
                WHEN "mozillavpn" THEN v.client_info.client_id
                ELSE v.metrics.uuid.session_installation_id
            END AS client_id,
            v.metrics.uuid.session_session_id AS session_id,
            v.metrics.datetime.session_session_start AS session_start,
            v.metrics.datetime.session_session_end AS session_end,
        FROM `moz-fx-data-shared-prod.mozilla_vpn.vpnsession` AS v

        UNION ALL
        SELECT 
            "daemonsession" AS source_table,
            DATE(d.submission_timestamp) AS submission_date,
            CONCAT(client_info.os, " - daemon") AS app,
            d.metrics.uuid.session_installation_id AS client_id,
            d.metrics.uuid.session_daemon_session_id AS session_id,
            d.metrics.datetime.session_daemon_session_start AS session_start,
            d.metrics.datetime.session_daemon_session_end AS session_end,
        FROM `moz-fx-data-shared-prod.mozilla_vpn.daemonsession` AS d
    )

    RIGHT JOIN
        (
            SELECT
                client_id AS population_client_id,
                submission_date AS population_submission_date,
                build_id AS population_build_id,
                app AS population_app,
                FROM
                population
            GROUP BY
                population_submission_date,
                population_client_id,
                population_build_id
                ,population_app
                ) AS p
    ON
        submission_date = p.population_submission_date
        
        AND client_id = p.population_client_id
        
        
        AND p.population_build_id IS NULL
        
        AND app = p.population_app
        
    WHERE
        
        DATE(submission_date) = DATE('2024-02-09')
        
    GROUP BY
        submission_date,
        build_id,
        client_id
        ,app
        
),


-- combine the metrics from all the data sources
joined_metrics AS (
  SELECT
    population.submission_date AS submission_date,
    population.client_id AS client_id,
    population.build_id,
    population.app AS app,
    
    population.branch AS branch,
    avg_session_duration,
        avg_sum_session_duration,
        active_subscribers,
        session_count,
        FROM population
  LEFT JOIN merged_metrics_session_duration_table
  ON
    merged_metrics_session_duration_table.submission_date = population.submission_date AND
    
    merged_metrics_session_duration_table.client_id = population.client_id AND
    
    (population.build_id IS NULL OR merged_metrics_session_duration_table.build_id = population.build_id)
    AND merged_metrics_session_duration_table.app = population.app
    
  LEFT JOIN merged_metrics_base_table
  ON
    merged_metrics_base_table.submission_date = population.submission_date AND
    
    merged_metrics_base_table.client_id = population.client_id AND
    
    (population.build_id IS NULL OR merged_metrics_base_table.build_id = population.build_id)
    AND merged_metrics_base_table.app = population.app
    
  
),

-- normalize histograms and apply filters
normalized_metrics AS (
    SELECT
        *
    FROM joined_metrics
    
)
SELECT
    * REPLACE(DATE('2024-02-09') AS submission_date)
FROM
    normalized_metrics


