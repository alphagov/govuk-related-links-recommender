SELECT
  source_content_id,
  destination_content_id,
  COUNT(*) AS weight
FROM (
  SELECT
    sessionId,
    hitNumber,
    source_node,
    destination_node
  FROM (
    SELECT
      hitNumber,
      CONCAT(fullVisitorId,"-",CAST(visitId AS STRING),"-",CAST(visitNumber AS STRING)) AS sessionId,
      content_id AS source_node,
      LEAD(content_id) OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber) AS destination_node
    FROM (
      SELECT
        fullVisitorId,
        visitId,
        visitNumber,
        visitStartTime,
        hits.hitNumber AS hitNumber,
        hits.type AS hit_type,
        hits.page.pagePath AS page_path,
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
                 _TABLE_SUFFIX BETWEEN @three_weeks_ago
                 AND @yesterday
        )
    WHERE
      hit_type = 'PAGE'
      AND page_path != '/'
      AND document_type NOT IN UNNEST(@excluded_document_types)
      AND content_id NOT IN ( '00000000-0000-0000-0000-000000000000',
        '[object Object]') )
  WHERE
    destination_node IS NOT NULL
    AND destination_node != source_node
  GROUP BY
    sessionId,
    hitNumber,
    source_node,
    destination_node)
GROUP BY
  source_node,
  destination_node
ORDER BY
  weight DESC