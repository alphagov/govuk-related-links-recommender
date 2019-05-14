import os
import pandas as pd
from datetime import datetime, timedelta
from src.utils.miscellaneous import get_excluded_document_types


class BigQueryDataExtractor:
    def __init__(self):
        self.data_dir = os.getenv('DATA_DIR')
        self.bq_key = os.getenv('BIG_QUERY_KEY')
        self.project_id = os.getenv('BIG_QUERY_PROJECT')

        self.blacklisted_document_types = ", ".join(get_excluded_document_types())

        self.yesterday = (datetime.today()- timedelta(1)).strftime('%Y%m%d')
        self.three_weeks_ago = (datetime.today()- timedelta(22)).strftime('%Y%m%d')

        # TODO this query gets content_ids but we may want to restrict to where Occurences>1 in the query rather than dropping later
        self.query = """SELECT
          COUNT(*) AS Occurrences,
          PageSeq_Length,
          PageSequence,
          CIDSequence
        FROM (
          SELECT
            *
          FROM (
            SELECT
              CONCAT(fullVisitorId,"-",CAST(visitId AS STRING),"-",CAST(visitNumber AS STRING)) AS sessionId,
              STRING_AGG(IF(htype = 'PAGE',
                  pagePath,
                  NULL),">>") OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS PageSequence,
               STRING_AGG(IF(htype = 'PAGE',
                  content_id,
                  NULL),">>") OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS CIDSequence,
              SUM(IF(htype='PAGE',
                  1,
                  0)) OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS PageSeq_Length
            FROM (
              SELECT
                fullVisitorId,
                visitId,
                visitNumber,
                visitStartTime,
                hits.page.pagePath AS pagePath,
                hits.hitNumber AS hitNumber,
                hits.type AS htype,
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
                _TABLE_SUFFIX BETWEEN """+f"'{self.three_weeks_ago}'"+" AND "+f"'{self.yesterday}'"+""")
            WHERE
              pagePath != '/'
              AND document_type NOT IN ("""+self.blacklisted_document_types+""")
              AND content_id NOT IN ( '00000000-0000-0000-0000-000000000000',
                '[object Object]'))
          WHERE
            PageSeq_Length >1
          GROUP BY
            sessionId,
            PageSequence,
            CIDSequence,
            PageSeq_Length)
        GROUP BY
          PageSequence,
          CIDSequence,
          PageSeq_Length"""

        # TODO this query not tested
        self.untested_query = """SELECT
          COUNT(*) AS Occurrences,
          PageSeq_Length,
          CIDSequence
        FROM (
          SELECT
            *
          FROM (
            SELECT
              CONCAT(fullVisitorId,"-",CAST(visitId AS STRING),"-",CAST(visitNumber AS STRING)) AS sessionId,
               STRING_AGG(IF(htype = 'PAGE',
                  content_id,
                  NULL),">>") OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS CIDSequence,
              SUM(IF(htype='PAGE',
                  1,
                  0)) OVER (PARTITION BY fullVisitorId, visitId, visitStartTime ORDER BY hitNumber ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS PageSeq_Length
            FROM (
              SELECT
                fullVisitorId,
                visitId,
                visitNumber,
                visitStartTime,
                hits.hitNumber AS hitNumber,
                hits.type AS htype,
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
                _TABLE_SUFFIX BETWEEN """+f"'{self.three_weeks_ago}'"+" AND "+f"'{self.yesterday}'"+""")
            WHERE
              pagePath != '/'
              AND document_type NOT IN ("""+self.blacklisted_document_types+""")
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
          PageSeq_Length"""

    def retrieve_data_from_big_query(self):
        # previous call for 2 weeks data cost $0.12
        return pd.read_gbq(self.query,
                            project_id=self.project_id,
                            reauth=False,
                            private_key=self.bq_key,
                            dialect="standard")


# TODO only save out columns needed downstream
# output_df.to_csv(os.path.join(DATA_DIR, "tmp",  "functional_network_data.csv"), index=False)


