from deltalake import DeltaTable

def upsert(new_data, target_table: DeltaTable, primary_key: list[str]):
    predicate = " AND ".join([f"target.{col} = source.{col}" for col in primary_key])
    return (
        target_table
        .merge(
            source=new_data,
            predicate=predicate,
            source_alias="source",
            target_alias="target"
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )