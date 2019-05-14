import pandas as pd

def test_retrieve_data_from_big_query():
    # previous call for 2 weeks data cost $0.12
    return pd.read_gbq(QUERY,
                        project_id=PROJECT_ID,
                        reauth=False,
                        private_key=BQ_KEY,
                        dialect="standard")


# TODO only save out columns needed downstream
# output_df.to_csv(os.path.join(DATA_DIR, "tmp",  "functional_network_data.csv"), index=False)


