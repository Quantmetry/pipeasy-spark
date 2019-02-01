import pipeasy_spark as ppz


def test_no_stages():
    pipeline = ppz.build_pipeline({})
    assert pipeline.getStages() == []

    pipeline = ppz.build_pipeline({
        'column_not_transformed': []
    })
    assert pipeline.getStages() == []


def test_column_name_doesnt_change(string_transformers):
    pipeline = ppz.build_pipeline({
        'column': string_transformers
    })
    stages = pipeline.getStages()
    first_stage, last_stage = stages[0], stages[-1]

    assert first_stage.getInputCol() == 'column'
    assert last_stage.getOutputCol() == 'column'
    assert isinstance(last_stage, ppz.transformers.ColumnRenamer)


def test_it_creates_a_working_pipeline(spark, string_transformers, number_transformers):
    df = spark.createDataFrame([
        ('Benjamin', 178.1, 0),
        ('Omar', 178.1, 1),
    ], schema=['name', 'height', 'target'])

    pipeline = ppz.build_pipeline({
        'name': string_transformers,
        'height': number_transformers
    })
    trained_pipeline = pipeline.fit(df)
    df_transformed = trained_pipeline.transform(df)

    def get_target_values(dataframe):
        return dataframe.rdd.map(lambda row: row['target']).collect()

    assert set(df.columns) == set(df_transformed.columns)
    assert df.count() == df_transformed.count()
    assert get_target_values(df) == get_target_values(df_transformed)
