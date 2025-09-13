from pandas import DataFrame, to_datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.FIX_SYNTAX_ERRORS

    This marks any improperly formatted values in each column specified
    as invalid.

    Docs: https://docs.mage.ai/guides/transformer-blocks#fix-syntax-errors
    """
    df.tpep_pickup_datetime = to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = to_datetime(df.tpep_dropoff_datetime)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
