#!/bin/bash

# Define o nome do bucket
bucket_name="767398038964-raw"

# Declara um array associativo para mapear prefixos para pastas de destino, focando apenas em 'green' e 'yellow'
declare -A prefixos_e_pastas=(
    ["raw/green_tripdata_"]="silver/green/"
    ["raw/yellow_tripdata_"]="silver/yellow/"
)

echo "Iniciando o script..."

# Itera sobre cada entrada no array associativo
for prefixo in "${!prefixos_e_pastas[@]}"; do
    pasta_destino=${prefixos_e_pastas[$prefixo]}

    # Lista todos os arquivos no bucket com o prefixo especificado
    # e itera sobre cada arquivo encontrado
    aws s3 ls "s3://$bucket_name/$prefixo" --recursive | while read -r line; do
        # Extrai o nome do arquivo do output do comando 'aws s3 ls'
        arquivo=$(echo $line | awk '{print $4}')
        if [ -z "$arquivo" ]; then
            continue # Se não houver arquivo, pula para a próxima iteração
        fi
        nome_arquivo=$(basename "$arquivo")

        # Extrai o ano do nome do arquivo e verifica se está entre 2019 e 2022
        ano=$(echo $nome_arquivo | grep -oP '\d{4}')
        if [[ $ano -ge 2019 && $ano -le 2022 ]]; then
            copiar_para="$pasta_destino$nome_arquivo"

            # Copia o arquivo para a nova localização no bucket
            aws s3 cp "s3://$bucket_name/$arquivo" "s3://$bucket_name/$copiar_para"

            echo "Arquivo $arquivo copiado para $copiar_para"
        fi
    done
done

echo "Script concluído."