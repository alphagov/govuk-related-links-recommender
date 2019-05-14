SELECT
          COUNT(*) AS Occurrences,
          PageSeq_Length,
          CIDSequence
        FROM (
          SELECT
            *
          FROM (
            SELECT
              CONCAT(fullVisitorId,"-",CAST(visitId AS STRING),"-",CAST(visitNumber AS STRING)) AS sessionId,
               STRING_AGG(IF(hit_type = 'PAGE',
                  content_id,
                  NULL),">>") OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS CIDSequence,
              SUM(IF(hit_type='PAGE',
                  1,
                  0)) OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS PageSeq_Length
            FROM (
              SELECT
                fullVisitorId,
                visitId,
                visitNumber,
                visitStartTime,
                hits.hitNumber AS hitNumber,
                hits.type AS hit_type,
                (
                SELECT
                  value
                FROM
                  hits.customDimensions
                WHERE
                  index=4) AS content_id,
                (
                SELECT
                  value
                FROM
                  hits.customDimensions
                WHERE
                  index=2) AS document_type
              FROM
                `govuk-bigquery-analytics.87773428.ga_sessions_*` AS sessions
              CROSS JOIN
                UNNEST(sessions.hits) AS hits
              WHERE
                _TABLE_SUFFIX BETWEEN """+'{three_weeks_ago}'"+" AND "+'{yesterday}'"+""")
            WHERE
              pagePath != '/'
              AND document_type NOT IN ("""+{blacklisted_document_types}+""")
              AND content_id NOT IN ( '00000000-0000-0000-0000-000000000000',
                '[object Object]'))
          WHERE
            PageSeq_Length >1 AND Occurrences >1
          GROUP BY
            sessionId,
            CIDSequence,
            PageSeq_Length)
        GROUP BY
          CIDSequence,
          PageSeq_Length