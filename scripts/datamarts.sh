#!/usr/bin/env bash

# obtém o diretório do script e seu diretório pai (diretório do projeto)
# e muda o diretório para execução
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJ_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJ_DIR" || exit

# cria as variáveis de ano de início e de fim e
# a lista de bases a serem processadas
anoi=2007
anof=$(date +%Y)
anof=$((anof - 1))
granularidade='escola'

# gera o for loop de execução do comando de processamento de dados
for ano in $(seq $anoi $anof)
do
  for gran in $granularidade
  do
    python run.py datamart processa-datamart--granularidade $gran --ano "$ano"
  done
done