import boto3

# Inicializa o cliente S3
s3 = boto3.client('s3')
bucket_name = '767398038964-raw'

# Lista dos prefixos de arquivos e pastas de destino
prefixos_e_pastas = {
    'raw/fhv_tripdata_': 'silver/fhv/',
    'raw/fhvhv_tripdata_': 'silver/fhvhv/',
    'raw/green_tripdata_': 'silver/green/',
    'raw/yellow_tripdata_': 'silver/yellow/',
}

print("to aqui")

# Itera sobre cada categoria de viagem
for prefixo, pasta_destino in prefixos_e_pastas.items():
    # Lista todos os arquivos no bucket com o prefixo especificado
    arquivos = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefixo)

    if 'Contents' in arquivos:
        for arquivo in arquivos['Contents']:
            nome_arquivo = arquivo['Key'].split('/')[-1]
            copiar_para = pasta_destino + nome_arquivo

            # Copia o arquivo para a nova localização
            s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': arquivo['Key']},
                           Key=copiar_para)
            print(f"Arquivo {arquivo['Key']} copiado para {copiar_para}")
