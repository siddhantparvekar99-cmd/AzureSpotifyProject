import dlt

expectations={

  "rule_1" : "user_id is NOT NULL"
}

@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
    return (
        spark.readStream.table("spotify_cata.silver.dimuser")
    )

dlt.create_streaming_table(
  name= "dimuser",
  expect_all_or_drop=expectations
)

dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type="2"
)