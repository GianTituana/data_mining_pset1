import pandas as pd
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here
    URL = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'
    output_name= 'raw_data.csv.gz'
    os.system(f'wget {URL} -O {output_name}')
 
    data_it = pd.read_csv(output_name, iterator=True,chunksize=100000)

    final_data=pd.DataFrame()

    i =   0

    while True:

        try:
            data_raw=next(data_it)
            if i==0:
                final_data = data_raw
            else: 
                pd.concat([final_data,data_raw], axis=0)
            i+=1

        except StopIteration:
            break  

    return  final_data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
