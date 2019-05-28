SELECT
  content_id,
  page_path,
  COUNT(*) AS page_hits
FROM
(
SELECT
visit_id,
hit_number,
page_path,
content_id
FROM
(
  SELECT
    visitId AS visit_id,
    hits.hitNumber AS hit_number,
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
    AND @yesterday)
WHERE
  page_path != '/'
  AND content_id NOT IN ('00000000-0000-0000-0000-000000000000',
    '[object Object]')
GROUP BY
visit_id,
hit_number,
page_path,
content_id
 )
GROUP BY
  content_id,
  page_path
ORDER BY
  page_hits DESC
LIMIT 100