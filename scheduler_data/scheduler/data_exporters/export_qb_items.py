from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path
import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    Exporta datos a PostgreSQL usando UPSERT fila por fila para garantizar idempotencia.
    Re-ejecutar con los mismos datos no duplicará filas.

    Docs: https://docs.mage.ai/design/data-loading#postgresql
    """
    if df.empty:
        print("DataFrame vacío, no hay datos para exportar")
        return
    
    schema_name = 'raw'
    table_name = 'qb_item'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    print(f"Exportando {len(df)} registros a {schema_name}.{table_name}")
    print("Método: UPSERT fila por fila")
    
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Limpiar cualquier transacción pendiente al inicio
        try:
            loader.execute("ROLLBACK;")
            print("Limpieza inicial de transacciones completada")
        except:
            pass  # Ignorar si no hay transacción pendiente
        
        # Verificar conexión
        try:
            test_result = loader.execute("SELECT version();")
            print(f"Conectado a PostgreSQL: {test_result[0][0] if test_result else 'Versión no disponible'}")
        except Exception as e:
            print(f"Error verificando conexión: {e}")
            return
        
        # Asumir que esquema 'raw' existe
        print(f"Usando esquema '{schema_name}' (asumiendo que existe)")
        
        # Verificar si la tabla existe
        verify_table_sql = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}';
        """
        
        try:
            table_exists = loader.execute(verify_table_sql)
            if table_exists and len(table_exists) > 0:
                print(f"✓ Tabla '{schema_name}.{table_name}' ya existe")
            else:
                print(f"Tabla '{schema_name}.{table_name}' no existe, creándola...")
                
                # Crear tabla si no existe
                create_table_sql = f"""
                CREATE TABLE {schema_name}.{table_name} (
                    id VARCHAR(50) PRIMARY KEY,
                    payload JSONB,
                    ingested_at_utc TIMESTAMPTZ,
                    extract_window_start_utc TIMESTAMPTZ,
                    extract_window_end_utc TIMESTAMPTZ,
                    page_number INTEGER,
                    page_size INTEGER,
                    request_payload JSONB
                );
                """
                
                result = loader.execute(create_table_sql)
                print(f"Comando CREATE TABLE ejecutado - Resultado: {result}")
                
                # Verificar nuevamente que se creó
                table_exists_after = loader.execute(verify_table_sql)
                if table_exists_after and len(table_exists_after) > 0:
                    print(f"Tabla '{schema_name}.{table_name}' creada exitosamente")
                else:
                    print(f"Tabla '{schema_name}.{table_name}' podría no haberse creado, pero continuando...")
                    
        except Exception as e:
            print(f"Error verificando/creando tabla '{schema_name}.{table_name}': {e}")
            print("Continuando con el procesamiento...")
        
        # Contadores para estadísticas
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        print(f"Procesando {len(df)} registros individualmente...")
        
        # Procesar cada fila del DataFrame
        for index_num, (index, row) in enumerate(df.iterrows()):
            try:
                # Preparar valores para la consulta, escapando comillas simples
                values_list = []
                update_clauses = []
                
                for col in df.columns:
                    value = row[col]
                    # Convertir valor a string SQL apropiado
                    if value is None or pd.isna(value):
                        sql_value = 'NULL'
                    elif isinstance(value, str):
                        # Escapar comillas simples y envolver en comillas
                        escaped_value = value.replace("'", "''")
                        sql_value = f"'{escaped_value}'"
                    else:
                        # Para otros tipos (números, etc.), convertir a string
                        sql_value = f"'{str(value)}'"
                    
                    values_list.append(sql_value)
                    
                    if col != 'id':  # No actualizar la clave primaria
                        update_clauses.append(f"{col} = EXCLUDED.{col}")
                
                # Construir consulta UPSERT con valores directos
                columns_str = ', '.join(df.columns)
                values_str = ', '.join(values_list)
                update_str = ', '.join(update_clauses)
                
                # UPSERT sin transacción explícita (Mage AI maneja las transacciones)
                upsert_sql = f"""
                INSERT INTO {schema_name}.{table_name} ({columns_str}) 
                VALUES ({values_str})
                ON CONFLICT (id) 
                DO UPDATE SET {update_str};
                """
                
                # Ejecutar UPSERT con manejo de transacción
                try:
                    loader.execute(upsert_sql)
                    inserted_count += 1
                except Exception as upsert_error:
                    # Si el UPSERT falla, hacer rollback inmediato
                    try:
                        loader.execute("ROLLBACK;")
                    except:
                        pass
                    raise upsert_error  # Re-lanzar para el manejo principal
                
                # Mostrar progreso cada 100 registros
                processed_count = index_num + 1
                if processed_count % 100 == 0:
                    print(f"Procesados: {processed_count}/{len(df)} registros")
                
            except Exception as e:
                error_count += 1
                record_id = row.get('id', 'N/A')
                record_num = index_num + 1
                print(f"Error procesando registro {record_num} (ID: {record_id}): {e}")
                
                # Intentar hacer rollback si hay transacción colgada
                try:
                    loader.execute("ROLLBACK;")
                except:
                    pass
                
                continue
        
        # Estadísticas finales
        try:
            final_count_sql = f"SELECT COUNT(*) FROM {schema_name}.{table_name};"
            final_result = loader.execute(final_count_sql)
            final_count = final_result[0][0] if final_result else 0
        except Exception as e:
            print(f"Error obteniendo estadísticas finales: {e}")
            # Intentar rollback y luego obtener estadísticas
            try:
                loader.execute("ROLLBACK;")
                final_result = loader.execute(final_count_sql)
                final_count = final_result[0][0] if final_result else 0
            except:
                final_count = "N/A"
        
        print(f"\nUPSERT FILA POR FILA COMPLETADO")
        print(f"Total registros procesados: {len(df)}")
        print(f"Registros insertados (nuevos): {inserted_count}")
        print(f"Registros actualizados (existentes): {updated_count}")
        print(f"Errores: {error_count}")
        print(f"Total registros en tabla: {final_count}")
        print("Idempotencia garantizada: re-ejecutar con los mismos IDs no duplicará filas")
