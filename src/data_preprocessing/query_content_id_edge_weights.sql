SELECT
  source_content_id,
  destination_content_id,
  COUNT(*) AS weight
FROM (
  SELECT
    sessionId,
    hitNumber,
    source_content_id,
    destination_content_id
  FROM (
    SELECT
      hitNumber,
      CONCAT(fullVisitorId,"-",CAST(visitId AS STRING),"-",CAST(visitNumber AS STRING)) AS sessionId,
      content_id AS source_content_id,
      LEAD(content_id) OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber) AS destination_content_id
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
                 _TABLE_SUFFIX BETWEEN @from_date
                 AND @to_date
        )
    WHERE
      hit_type = 'PAGE'
      AND page_path != '/'
      AND document_type NOT IN UNNEST(@excluded_document_types)
      AND content_id NOT IN ( '00000000-0000-0000-0000-000000000000',
        '[object Object]') )
  WHERE
    destination_content_id IS NOT NULL
    AND destination_content_id != source_content_id
  GROUP BY
    sessionId,
    hitNumber,
    source_content_id,
    destination_content_id)
GROUP BY
  source_content_id,
  destination_content_id
HAVING
  weight >= @weight_threshold
