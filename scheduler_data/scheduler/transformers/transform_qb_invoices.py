import pandas as pd
import json

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Transforma all_pages_metadata a un DataFrame donde cada fila es un invoice individual.

    Args:
        data: all_pages_metadata del script de ingesta

    Returns:
        pandas.DataFrame: DataFrame con una fila por invoice
    """
    # Lista para almacenar todas las filas del DataFrame
    rows = []
    
    # Procesar cada página
    for page_metadata in data:
        # Extraer metadatos comunes de la página
        page_common_data = {
            'ingested_at_utc': page_metadata.get('ingested_at_utc'),
            'extract_window_start_utc': page_metadata.get('extract_window_start_utc'),
            'extract_window_end_utc': page_metadata.get('extract_window_end_utc'),
            'page_number': page_metadata.get('page_number'),
            'page_size': page_metadata.get('page_size'),
            'request_payload': json.dumps(page_metadata.get('request_payload', {}))
        }
        
        # Procesar cada invoice en la página
        invoices = page_metadata.get('invoices', [])
        
        for invoice in invoices:
            # Crear fila para este invoice
            row = {
                'id': invoice.get('Id'),  # ID del invoice
                'payload': json.dumps(invoice),  # Payload completo del invoice como JSON
                **page_common_data  # Agregar metadatos comunes de la página
            }
            
            rows.append(row)
    
    # Crear DataFrame
    df = pd.DataFrame(rows)
    
    # Reordenar columnas en el orden deseado
    column_order = [
        'id',
        'payload', 
        'ingested_at_utc',
        'extract_window_start_utc',
        'extract_window_end_utc',
        'page_number',
        'page_size',
        'request_payload'
    ]
    
    df = df[column_order]
    
    print(f"DataFrame creado con {len(df)} invoices de {len(data)} páginas")
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
