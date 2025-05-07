import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import xlsxwriter
import numpy as np
from datetime import datetime

def get_better_type_info(df, column_name, arrow_schema):
    """Determina um tipo mais específico para uma coluna"""
    # Obter tipo pandas
    pandas_type = df[column_name].dtype
    
    # Para tipos object, tentar detectar tipos mais específicos
    if pandas_type == 'object':
        # Tentar encontrar o campo no schema Arrow
        arrow_field = None
        try:
            for field in arrow_schema:
                if field.name == column_name:
                    arrow_field = field
                    break
        except:
            pass
            
        if arrow_field and 'string' in str(arrow_field.type).lower():
            # É uma string no Arrow
            return 'string'
            
        # Verificar se é data/hora
        non_null_values = df[column_name].dropna()
        if len(non_null_values) > 0:
            try:
                # Verificar se todas as strings podem ser convertidas para data
                sample = non_null_values.iloc[0]
                if isinstance(sample, str):
                    try:
                        pd.to_datetime(non_null_values)
                        return 'datetime'
                    except:
                        pass
                    
                    # Verificar formatos específicos
                    date_formats = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d']
                    for fmt in date_formats:
                        try:
                            datetime.strptime(sample, fmt)
                            return f'date (formato: {fmt})'
                        except:
                            pass
            except:
                pass
                
    # Para tipos numéricos, especificar inteiro vs. decimal
    if pd.api.types.is_integer_dtype(pandas_type):
        return 'integer'
    if pd.api.types.is_float_dtype(pandas_type):
        # Verificar se os valores são todos inteiros
        non_null = df[column_name].dropna()
        if len(non_null) > 0 and all(non_null.apply(lambda x: x == int(x))):
            return 'integer (armazenado como float)'
        return 'decimal'
        
    # Para booleanos
    if pd.api.types.is_bool_dtype(pandas_type):
        return 'boolean'
        
    # Para tipos datetime
    if pd.api.types.is_datetime64_dtype(pandas_type):
        return 'datetime'
        
    # Retornar o tipo original se nenhuma melhoria for possível
    return str(pandas_type)

def extract_parquet_metadata(parquet_files):
    metadata_list = []
    
    for file_path in parquet_files:
        file_name = os.path.basename(file_path)
        
        # Ler o arquivo usando pandas e pyarrow
        df = pd.read_parquet(file_path)
        parquet_file = pq.ParquetFile(file_path)
        arrow_schema = parquet_file.schema_arrow
        
        # Para cada campo, extrair as informações
        for field_name in df.columns:
            field_type = get_better_type_info(df, field_name, arrow_schema)
            
            # Estatísticas básicas para ajudar na validação
            stats = {}
            try:
                if field_type in ['integer', 'decimal', 'integer (armazenado como float)']:
                    stats['min'] = str(df[field_name].min())
                    stats['max'] = str(df[field_name].max())
                    # Verificar se é um possível ID/código
                    if field_name.lower().endswith('id') or 'codigo' in field_name.lower():
                        if df[field_name].nunique() == len(df):
                            stats['observação'] = 'possível chave única'
                elif field_type == 'string':
                    # Verificar padrões comuns em strings
                    sample_values = df[field_name].dropna().sample(min(5, len(df))).tolist()
                    if all(isinstance(val, str) and len(val) == 36 and '-' in val for val in sample_values):
                        stats['observação'] = 'possível UUID'
                    elif field_name.lower().endswith('_iso') or all(isinstance(val, str) and 'T' in val and 'Z' in val for val in sample_values):
                        stats['observação'] = 'possível timestamp ISO'
            except:
                pass
                
            # Verificar nulos
            null_count = df[field_name].isna().sum()
            if null_count > 0:
                stats['nulos'] = f"{null_count} ({(null_count/len(df))*100:.1f}%)"
                
            # Adicionar observações como string
            observations = '; '.join([f"{k}: {v}" for k, v in stats.items()])
            
            # Adicionar à lista de metadados
            metadata_list.append({
                'arquivo': file_name,
                'campo': field_name,
                'tipo': field_type,
                'observações': observations,
                'descrição': ''  # Vazio por padrão, a ser preenchido manualmente
            })
    
    return metadata_list

def create_excel_from_metadata(metadata_list, output_file='parquet_metadata.xlsx'):
    # Criar DataFrame com metadados
    df = pd.DataFrame(metadata_list)
    
    # Criar arquivo Excel
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Metadados', index=False)
        
        # Obter objeto workbook e worksheet
        workbook = writer.book
        worksheet = writer.sheets['Metadados']
        
        # Adicionar formato para cabeçalho
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'bg_color': '#D7E4BC',
            'border': 1
        })
        
        # Aplicar formato nos cabeçalhos
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Ajustar largura das colunas
        worksheet.set_column('A:A', 30)  # Arquivo
        worksheet.set_column('B:B', 30)  # Campo
        worksheet.set_column('C:C', 20)  # Tipo
        worksheet.set_column('D:D', 40)  # Observações
        worksheet.set_column('E:E', 50)  # Descrição
    
    return output_file

def process_parquet_files(parquet_files, output_excel='parquet_metadata.xlsx'):
    metadata = extract_parquet_metadata(parquet_files)
    excel_file = create_excel_from_metadata(metadata, output_excel)
    return excel_file

if __name__ == "__main__":
    # Exemplo de uso
    parquet_dir = "."  # Diretório com arquivos parquet
    parquet_files = [os.path.join(parquet_dir, f) for f in os.listdir(parquet_dir) if f.endswith('.parquet')]
    
    if parquet_files:
        output_file = process_parquet_files(parquet_files)
        print(f"Arquivo Excel criado: {output_file}")
    else:
        print("Nenhum arquivo Parquet encontrado no diretório.")