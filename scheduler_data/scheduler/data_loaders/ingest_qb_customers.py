import requests
import json
import pandas as pd
from datetime import datetime, timedelta
from mage_ai.data_preparation.shared.secrets import get_secret_value
import time


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def _refrescar_access_token():

    refresh_token = get_secret_value('qb_refresh_token')
    client_id = get_secret_value('qb_client_id')
    client_secret = get_secret_value('qb_client_secret')
    
    url_base = 'https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'application/json'
    }
    
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': client_id,
        'client_secret': client_secret
    }
    
    try:
        print('Post para refrescar token')
        response = requests.post(url_base, headers=headers, data=data, timeout=60)
        response.raise_for_status()
        token_data = response.json()
        new_access_token = token_data.get('access_token')
        new_refresh_token = token_data.get('refresh_token')

        if new_access_token:
            print('Exito al refrescar token')
            return new_access_token, new_refresh_token
        else:
            raise ValueError("Error al solicitar nuevo token")
            
    except requests.exceptions.RequestException as e:
        print(f'Error al refrescar token: {e}')
        return None, None
    except json.JSONDecodeError as e:
        print(f'Error al decodificar respuesta JSON: {e}')
        return None, None

def _fetch_qb_data(realm_id, access_token, query, base_url, minor_version, start_position=1, max_results=1000):

    if not base_url or not minor_version:
        raise ValueError("Se requiere una URL base y el minor version")    

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'Content-Type': 'text/plain'
    }

    # paginación 
    paginated_query = f"{query} STARTPOSITION {start_position} MAXRESULTS {max_results}"

    params = {
        'query': paginated_query,
        'minorversion': minor_version
    }

    url = f"{base_url.rstrip('/')}/v3/company/{realm_id}/query"
    
    #  reintentos con backoff exponencial
    max_retries = 5
    base_timeout = 60
    base_delay = 1  # delay inicial para backoff exponencial
    
    # circuit breaker
    consecutive_failures = getattr(_fetch_qb_data, '_consecutive_failures', 0)
    circuit_open = getattr(_fetch_qb_data, '_circuit_open_until', 0)
    
    # Verificar si circuit breaker está abierto
    current_time = time.time()
    if current_time < circuit_open:
        remaining_time = int(circuit_open - current_time)
        print(f'CIRCUIT BREAKER ABIERTO - Esperando {remaining_time}s más antes de reintentar')
        time.sleep(min(remaining_time, 30))  # Esperar máximo 30s en esta llamada
        return None
    
    for attempt in range(max_retries):
        # timeout incremental
        current_timeout = base_timeout + (attempt * 30)
        # backoff exponential delay (solo después del primer intento)
        if attempt > 0:
            delay = base_delay * (2 ** (attempt - 1))  # 1s, 2s, 4s, 8s, 16s
            print(f'Backoff exponencial: esperando {delay}s antes del intento {attempt + 1}')
            time.sleep(delay)
        
        try:
            print(f'Intento {attempt + 1}/{max_retries} - Request al API')
            print(f'URL: {base_url}')
            print(f'Query: {paginated_query}')
            print(f'Posición: {start_position}, Máximo: {max_results}')
            print(f'Timeout: {current_timeout}s')
            print(f'Fallos consecutivos: {consecutive_failures}')
            
            response = requests.get(url, headers=headers, params=params, timeout=current_timeout)
            
            # manejo de rate limits
            if response.status_code == 429:  # Too Many Requests
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f'RATE LIMIT EXCEDIDO - Esperando {retry_after}s (HTTP 429)')
                time.sleep(retry_after)
                continue  # Reintentar sin contar como fallo
            
            if response.status_code == 401:
                print('Token expirado, refrescando...')
                new_access_token, new_refresh_token = _refrescar_access_token()
                
                if new_access_token:
                    headers['Authorization'] = f'Bearer {new_access_token}'
                    print('Token refrescado, reintentando...')
                    response = requests.get(url, headers=headers, params=params, timeout=current_timeout)
                else:
                    raise ValueError("Error crítico: No se pudo refrescar el token")
            
            response.raise_for_status()
            data = response.json()
            
            # ÉXITO - Resetear circuit breaker
            _fetch_qb_data._consecutive_failures = 0
            _fetch_qb_data._circuit_open_until = 0
            
            print(f'Datos recibidos exitosamente en intento {attempt + 1}')
            print(f'Página desde posición {start_position} obtenida correctamente')
            return data
            
        except requests.exceptions.Timeout as e:
            consecutive_failures += 1
            print(f'TIMEOUT en intento {attempt + 1} después de {current_timeout}s: {e}')
            _handle_failure(attempt, max_retries, consecutive_failures, 'TIMEOUT')
                
        except requests.exceptions.ConnectionError as e:
            consecutive_failures += 1
            print(f'ERROR DE CONEXIÓN en intento {attempt + 1}: {e}')
            _handle_failure(attempt, max_retries, consecutive_failures, 'CONNECTION_ERROR')
                
        except requests.exceptions.RequestException as e:
            consecutive_failures += 1
            print(f'ERROR DE REQUEST en intento {attempt + 1}: {e}')
            _handle_failure(attempt, max_retries, consecutive_failures, 'REQUEST_ERROR')
                
        except json.JSONDecodeError as e:
            consecutive_failures += 1
            print(f'ERROR DE JSON en intento {attempt + 1}: {e}')
            _handle_failure(attempt, max_retries, consecutive_failures, 'JSON_ERROR')
                
        except Exception as e:
            consecutive_failures += 1
            print(f'ERROR INESPERADO en intento {attempt + 1}: {e}')
            _handle_failure(attempt, max_retries, consecutive_failures, 'UNEXPECTED_ERROR')
    
    # todos los reintentos fallaron
    _fetch_qb_data._consecutive_failures = consecutive_failures
    _activate_circuit_breaker(consecutive_failures)
    print(f'FALLO TOTAL: {max_retries} intentos agotados. Fallos consecutivos: {consecutive_failures}')
    return None

def _handle_failure(attempt, max_retries, consecutive_failures, error_type):
    if attempt == max_retries - 1:  # Último intento
        print(f'ÚLTIMO INTENTO FALLADO - Tipo: {error_type}')
        print(f'Fallos consecutivos acumulados: {consecutive_failures}')
    else:
        next_delay = 1 * (2 ** attempt)  # Próximo delay exponencial
        print(f'Preparando reintento con backoff exponencial de {next_delay}s')

def _activate_circuit_breaker(consecutive_failures):
    if consecutive_failures >= 10:
        circuit_open_time = 300
        print('CIRCUIT BREAKER CRÍTICO: 10+ fallos consecutivos - Pausando 5 minutos')
    elif consecutive_failures >= 5:
        circuit_open_time = 60
        print('CIRCUIT BREAKER ACTIVADO: 5+ fallos consecutivos - Pausando 1 minuto')
    else:
        return  
    
    _fetch_qb_data._circuit_open_until = time.time() + circuit_open_time
    _fetch_qb_data._consecutive_failures = consecutive_failures

@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.
    
    Args:
        fecha_inicio (str): Fecha de inicio en formato YYYY-MM-DD (requerido para backfill)
        fecha_fin (str): Fecha de fin en formato YYYY-MM-DD (requerido para backfill)
        chunk_days (int): Número de días por chunk (opcional, default: 7)

    Returns:
        pandas.DataFrame: DataFrame con una fila por customer
    """
    realm_id = get_secret_value('qb_realm_id')
    access_token = get_secret_value('qb_access_token')
    minor_version = 75
    base_url = 'https://sandbox-quickbooks.api.intuit.com'
    
    print("REFRESCANDO TOKEN")
    new_access_token, new_refresh_token = _refrescar_access_token()
    
    if new_access_token:
        access_token = new_access_token  
        print(f"Token refrescado con exito")
    else:
        print("Error al regrescar el token, puede estar expirado")
    
    start_date_str = kwargs.get('fecha_inicio')
    end_date_str = kwargs.get('fecha_fin')
    chunk_days = kwargs.get('chunk_days', 7) 
    
    # variables de recuperacion
    resume_mode = kwargs.get('resume_mode', False)  # True para reanudar desde último exitoso
    retry_failed_chunks = kwargs.get('retry_failed_chunks', False)  # True para reintentar fallos
    verify_only = kwargs.get('verify_only', False)  # True para solo verificar sin procesar
    skip_chunks = kwargs.get('skip_chunks', [])  # Lista de números de chunk a omitir
    force_chunks = kwargs.get('force_chunks', [])  # Lista de números de chunk a forzar reproceso
    
    print(f"CONFIGURACIÓN DE PROCESAMIENTO")
    print(f"Resume mode: {'ACTIVADO' if resume_mode else 'DESACTIVADO'}")
    print(f"Retry failed chunks: {'ACTIVADO' if retry_failed_chunks else 'DESACTIVADO'}")
    print(f"Verify only: {'Solo verificación' if verify_only else 'Procesamiento normal'}")
    print(f"Skip chunks: {skip_chunks if skip_chunks else 'Ninguno'}")
    print(f"Force chunks: {force_chunks if force_chunks else 'Ninguno'}")
    
    if not start_date_str or not end_date_str:
        raise ValueError("Se requieren los parámetros 'fecha_inicio' y 'fecha_fin' en formato YYYY-MM-DD")
    
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(f"Formato de fecha inválido. Use YYYY-MM-DD: {e}")
    
    if start_date > end_date:
        raise ValueError("La fecha de inicio debe ser menor o igual a la fecha de fin")
    
    # timestamp de ingesta
    ingested_at_utc = kwargs.get('execution_date', datetime.utcnow())
    
    # Convertir a string si es datetime
    if isinstance(ingested_at_utc, datetime):
        ingested_at_utc_str = ingested_at_utc.isoformat() + 'Z'
    else:
        ingested_at_utc_str = str(ingested_at_utc)
    
    print(f'INICIO DEL BACKFILL DE QB customers')
    print(f'Rango completo: {start_date_str} a {end_date_str}')
    print(f'Chunk size: {chunk_days} días')
    print(f'Ingested at: {ingested_at_utc_str}')
    
    chunks = []
    current_date = start_date
    chunk_number = 1
    
    while current_date <= end_date:
        chunk_end = min(current_date + timedelta(days=chunk_days - 1), end_date)
        chunks.append({
            'chunk_number': chunk_number,
            'start_date': current_date,
            'end_date': chunk_end,
            'start_date_str': current_date.strftime('%Y-%m-%d'),
            'end_date_str': chunk_end.strftime('%Y-%m-%d')
        })
        current_date = chunk_end + timedelta(days=1)
        chunk_number += 1
    
    print(f'Total de chunks a procesar: {len(chunks)}')
    
    # tracking de progreso y recuperacion
    progress_tracker = {
        'run_id': f"{start_date_str}_{end_date_str}_{chunk_days}d_{ingested_at_utc_str.split('T')[0]}",
        'total_chunks': len(chunks),
        'completed_chunks': [],
        'failed_chunks': [],
        'skipped_chunks': skip_chunks,
        'processing_start': datetime.utcnow().isoformat() + 'Z'
    }
    
    # resume/retry
    chunks_to_process = []
    for chunk in chunks:
        chunk_num = chunk['chunk_number']
        
        # Verificar si saltar este chunk
        if chunk_num in skip_chunks:
            print(f"Saltando chunk {chunk_num} (en skip_chunks)")
            progress_tracker['skipped_chunks'].append(chunk_num)
            continue
            
        # Verificar si forzar reproceso
        if force_chunks and chunk_num in force_chunks:
            print(f"Forzando reproceso del chunk {chunk_num}")
            chunks_to_process.append(chunk)
            continue
        
        # modo verify_only, solo mostrar qué se haría
        if verify_only:
            print(f"[VERIFY] Chunk {chunk_num}: {chunk['start_date_str']} a {chunk['end_date_str']}")
            continue
            
        if resume_mode:
            print(f"[RESUME] Verificando chunk {chunk_num}...")
        
        chunks_to_process.append(chunk)
    
    if verify_only:
        print(f"\nVERIFICACIÓN COMPLETADA")
        print(f"Total chunks definidos: {len(chunks)}")
        print(f"Chunks a saltar: {len(skip_chunks)}")
        print(f"Chunks a forzar: {len(force_chunks)}")
        print(f"Chunks que se procesarían: {len(chunks_to_process)}")
        return pd.DataFrame()  # DataFrame vacío en modo verificación
    
    print(f"RESUMEN DE PROCESAMIENTO:")
    print(f"  Total chunks definidos: {len(chunks)}")
    print(f"  Chunks a procesar: {len(chunks_to_process)}")
    print(f"  Chunks a saltar: {len(skip_chunks)}")
    print(f"  Chunks forzados: {len(force_chunks) if force_chunks else 0}")
    
    # Lista para almacenar todas las filas del DataFrame
    all_rows = []
    total_customers = 0
    total_pages = 0
    processed_chunks_count = 0
    
    for chunk in chunks_to_process:
        chunk_start_time = time.time()
        processed_chunks_count += 1
        
        print(f'\nPROCESANDO CHUNK {chunk["chunk_number"]}/{len(chunks)} ({processed_chunks_count}/{len(chunks_to_process)} a procesar)')
        print(f'Fechas procesadas: {chunk["start_date_str"]} a {chunk["end_date_str"]}')
        
        try:
            # query para el chunk actual
            start_utc = f"{chunk['start_date_str']}T00:00:00Z"
            end_utc = f"{chunk['end_date_str']}T23:59:59Z"
            
            query = f"select * from Customer where MetaData.LastUpdatedTime >= '{start_utc}' and MetaData.LastUpdatedTime <= '{end_utc}'"
            
            # páginas para este chunk
            chunk_customers = 0
            chunk_pages = 0
            max_results = 100
            start_position = 1
            page_number = 1
            
            while True:
                page_start_time = time.time()
                
                # página actual
                data = _fetch_qb_data(
                    realm_id=realm_id,
                    access_token=access_token,
                    query=query,
                    base_url=base_url,
                    minor_version=minor_version,
                    start_position=start_position,
                    max_results=max_results
                )
                
                if not data or 'QueryResponse' not in data:
                    print(f'  No se encontraron más datos en página {page_number}')
                    break
                    
                query_response = data['QueryResponse']
                
                if 'Customer' not in query_response:
                    print(f'  No se encontraron customers en página {page_number}')
                    break
                    
                customers = query_response['Customer']
                page_end_time = time.time()
                page_duration = page_end_time - page_start_time
                
                # URL completa de la llamada API
                paginated_query = f"{query} STARTPOSITION {start_position} MAXRESULTS {max_results}"
                full_api_url = f"{base_url.rstrip('/')}/v3/company/{realm_id}/query?query={paginated_query}&minorversion={minor_version}"
                
                # Metadatos comunes de la página
                page_common_data = {
                    'ingested_at_utc': ingested_at_utc_str,
                    'extract_window_start_utc': start_utc,
                    'extract_window_end_utc': end_utc,
                    'page_number': page_number,
                    'page_size': len(customers),
                    'request_payload': json.dumps({
                        'full_api_url': full_api_url,
                        'method': 'GET',
                        'headers': {
                            'Authorization': 'Bearer [HIDDEN]',
                            'Accept': 'application/json',
                            'Content-Type': 'text/plain'
                        },
                        'query_parameters': {
                            'query': paginated_query,
                            'minorversion': minor_version
                        },
                        'base_url': base_url,
                        'realm_id': realm_id,
                        'original_query': query
                    })
                }
                
                # Procesar cada customer en la página y agregar al DataFrame
                for customer in customers:
                    row = {
                        'id': customer.get('Id'),
                        'payload': json.dumps(customer),
                        **page_common_data
                    }
                    all_rows.append(row)
                
                chunk_customers += len(customers)
                chunk_pages += 1
                
                print(f'  Página {page_number}: {len(customers)} customers en {page_duration:.2f}s')
                
                # si recibimos menos registros de los solicitados, es la última página
                if len(customers) < max_results:
                    break
                    
                # avanzar a la siguiente página
                start_position += max_results
                page_number += 1
            
            chunk_end_time = time.time()
            chunk_duration = chunk_end_time - chunk_start_time
            
            # Actualizar totales
            total_customers += chunk_customers
            total_pages += chunk_pages
            
            # Marcar chunk como completado exitosamente
            progress_tracker['completed_chunks'].append(chunk['chunk_number'])
            
            # LOGS DEL TRAMO COMPLETADO
            print(f'\nCHUNK {chunk["chunk_number"]} COMPLETADO')
            print(f'Fechas procesadas: {chunk["start_date_str"]} a {chunk["end_date_str"]}')
            print(f'Páginas leídas: {chunk_pages}')
            print(f'Filas insertadas: {chunk_customers}')
            print(f'Duración total del chunk: {chunk_duration:.2f} segundos')
            print(f'Promedio por página: {chunk_duration/max(chunk_pages, 1):.2f} segundos')
            print(f'Velocidad de ingesta: {chunk_customers/max(chunk_duration, 0.1):.2f} customers/segundo')
            print(f'Progreso general: {chunk["chunk_number"]}/{len(chunks)} chunks ({(chunk["chunk_number"]/len(chunks)*100):.1f}%)')
            print(f'Total acumulado hasta ahora: {total_customers} customers en {total_pages} páginas')
            print('-' * 60)
        
        except Exception as chunk_error:
            # Manejo de errores de chunk completo
            chunk_end_time = time.time()
            chunk_duration = chunk_end_time - chunk_start_time
            
            print(f'\nERROR EN CHUNK {chunk["chunk_number"]}')
            print(f'Fechas afectadas: {chunk["start_date_str"]} a {chunk["end_date_str"]}')
            print(f'Error: {str(chunk_error)}')
            print(f'Duración antes del error: {chunk_duration:.2f} segundos')
            
            # Marcar chunk como fallido
            progress_tracker['failed_chunks'].append({
                'chunk_number': chunk['chunk_number'],
                'date_range': f"{chunk['start_date_str']} a {chunk['end_date_str']}",
                'error': str(chunk_error),
                'duration': chunk_duration
            })
            
            # Decidir si continuar o fallar completamente
            if retry_failed_chunks:
                print(f"Chunk fallido marcado para reintento posterior")
            else:
                print(f"Continuando con siguiente chunk (chunk fallido omitido)")
            
            # Continuar con el siguiente chunk
            continue
    
    print(f'\nBACKFILL COMPLETADO')
    print(f'Total chunks procesados: {len(chunks)}')
    print(f'Total páginas: {total_pages}')
    print(f'Rango procesado: {start_date_str} a {end_date_str}')
    
    # Crear DataFrame final
    df = pd.DataFrame(all_rows)
    
    # eliminar duplicados:  esto puede ocurrir cuando customers caen en dos rangos de fecha de consultas
    if not df.empty:
        filas_antes = len(df)
        df = df.drop_duplicates(subset=['id'], keep='first')  # Mantener la primera ocurrencia
        filas_despues = len(df)
        duplicados_eliminados = filas_antes - filas_despues
    
    # reordenar columnas en el orden deseado 
    if not df.empty:
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

        available_columns = [col for col in column_order if col in df.columns]
        df = df[available_columns]

    print(f'Total customers(luego de eliminar duplicados): {len(df)}')
    print(f"\nDataFrame creado con {len(df)} customers")
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
