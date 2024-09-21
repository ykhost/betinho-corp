#!/bin/bash

# Verifica se o nome do arquivo foi fornecido
if [ "$#" -ne 1 ]; then
    echo "Uso: $0 <nome_do_arquivo.py>"
    exit 1
fi

# Nome do arquivo Python
ARQUIVO_PY="$1"

# Executa o arquivo Python no servidor remoto via SSH
ssh spark-master@localhost:22 "python3 /home/user/PROJETO_BETINHO_CORP/$ARQUIVO_PY"
