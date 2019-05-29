SELECT
  content_id,
  COUNT(*) AS page_hits
FROM
(
SELECT
visit_id,
hit_number,
content_id
FROM
(
  SELECT
    visitId AS visit_id,
    hits.hitNumber AS hit_number,
    hits.page.pagePath as page_path,
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
    AND @yesterday)
WHERE
  page_path != '/'
  AND content_id NOT IN ('00000000-0000-0000-0000-000000000000',
    '[object Object]')
GROUP BY
visit_id,
hit_number,
content_id
 )
GROUP BY
  content_id
HAVING page_hits > 5
ORDER BY
  page_hits DESC