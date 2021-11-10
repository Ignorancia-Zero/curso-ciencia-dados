# Curso de Ciência de Dados

---
## Sobre o Projeto

TODO

---
## Requisitos
* Python 3.9 64-bit
  * Windows: https://www.python.org/downloads/windows/ ou https://docs.anaconda.com/anaconda/install/windows/
  * Mac: https://www.python.org/downloads/mac-osx/ ou https://docs.anaconda.com/anaconda/install/mac-os/ ou https://formulae.brew.sh/formula/python@3.9
  * Linux: 
* rar/unrar
  * Windows: Na pasta de suporte, os executáveis já estão contidos. 
Entretanto se houver algum problema de incompatibilidade baixar de XXX 
e instalar o programa. Uma vez instalado, ir até a pasta de instalação 
e copiar os mesmos arquivos contidos em Suporte/WinRAR
  * Mac:
    1. Download unrar em https://www.rarlab.com/download.htm
    2. Descompacte o arquivo numa pasta local
    3. Navegue até a pasta e execute 

           sudo install -c -o $USER rar /usr/local/bin/
           sudo install -c -o $USER unrar /usr/local/bin/
  * Linux: ```sudo apt-get install unrar```
---
  
# Instalação do Ambiente Python

---
## Windows

### Configuração do Ambiente Python

#### Opção 1: YAML Anaconda
Recomendamos que você instale o anaconda (https://docs.anaconda.com/anaconda/install/windows/)
e execute o comando
```
conda env create -f setup_anaconda.yml
```
Para criar o ambiente com todos os pacotes necessário

*NOTA: Para executar o theano com suporte para C no Windows é necessário ainda executar:*
```
conda install -c conda-forge m2w64-toolchain_win-64
conda install -c anaconda libpython
```

#### Opção 2: Anaconda/Miniconda
* Execute no terminal
```
conda create -n curso-ciencia-dados python=3.9
```
* Em seguida execute `conda activate curso-ciencia-dados`. 
* Você pode adicionar esse ambiente ao Pycharm:
File -> Settings -> Project Interpreter -> Add -> Conda Environment ->
Existing Environment.


#### Opção 3: PyCharm com ambiente Conda
* Faça o download do PyCharm (https://www.jetbrains.com/pycharm/)
* Pycharm -> Settings -> Project Interpreter -> Add -> Conda Enviroment
-> [Selecione o conda apropriado] -> Python Version 3.9



### Instalação Pacotes

#### !!! IMPORTANTE !!! ####
Garanta que você possuí a última versão do Visual C++ runtime instalada 
(https://aka.ms/vs/16/release/vc_redist.x64.exe)

No Windows há um sério problema na configuração de ambiente
que ao ser criado pelo processo acima pode acarretar na geração
de problemas de compatibilidade principalmente com a biblioteca 
do geopandas. 

Desta forma, caso haja algum problema recomendamos a instalação 
manual do ambiente executando os comandos descritos abaixo a 
partir da raíz do projeto:
```
conda create -n curso-ciencia-dados python=3.9
conda activate curso-ciencia-dados
cd '.\suporte\Pacotes Windows\'
conda update -n base -c defaults conda
conda install -c anaconda numpy==1.21.2
conda install -c anaconda pandas==1.3.4
conda install -c conda-forge osmnx==1.1.1
conda install -c conda-forge rtree==0.9.4
conda install -c conda-forge matplotlib==3.4.3
conda install -c anaconda seaborn==0.11.0
conda install -c conda-forge pyshp==2.1.3
conda install -c conda-forge mercantile==1.2.1
conda install -c conda-forge geographiclib==1.52
conda install -c conda-forge geopy==2.2.0
conda install -c conda-forge contextily==1.2.0
pip install .\Cartopy-0.20.1-cp39-cp39-win_amd64.whl
pip install geoplot==0.4.4
conda install -c anaconda jupyter==1.0.0
conda install -c conda-forge jupyter_contrib_nbextensions==0.5.1
conda install -c conda-forge pyarrow==6.0.0
conda install -c plotly plotly==5.3.1
conda install -c conda-forge cufflinks-py==0.17.3
conda install -c anaconda beautifulsoup4==4.9.3
conda install -c anaconda boto3==1.18.21
pip install charamel==1.0.0
conda install -c conda-forge click==8.0.3
conda install -c conda-forge frozendict==2.0.3
conda install -c anaconda lxml==4.6.3
conda install -c conda-forge openpyxl==3.0.9
conda install -c conda-forge patool==1.12
conda install -c conda-forge pydrive2==1.10.0
pip install pyunpack==0.2.2
pip install rarfile==4.0
conda install -c anaconda yaml==5.4.1
conda install -c anaconda requests==2.26.0
conda install -c anaconda statsmodels==0.13.0
conda install -c conda-forge tqdm==4.62.3
conda install -c conda-forge black==21.9b0
conda install -c anaconda pytest==6.2.4
conda install -c conda-forge mypy==0.910
conda install -c conda-forge boto3-stubs==1.19.3
pip install botocore-stubs==1.22.3
conda install -c conda-forge mypy_boto3_ec2==1.20.1
conda install -c conda-forge mypy-boto3-s3==1.20.1
conda install -c conda-forge pandas-stubs==1.2.0.37
conda install -c conda-forge types-frozendict==2.0.1
conda install -c conda-forge types-requests==2.26.0
conda install -c conda-forge types-setuptools==57.4.2
conda install -c conda-forge types-python-dateutil==2.8.2
conda install -c conda-forge types-pytz==2021.3.0
conda install -c conda-forge types-pyyaml==5.4.12
```

### Mac

#### virtualenv
1. Instale a biblioteca `virtualenv` no Python.
1. Crie um ambiente virtual na raiz do repositório
1. Instale os pacotes requeridos
```
env $ pip3 install virtualenv
env $ python3 -m venv venv
env $ source ./venv/bin/activate
env $ pip install -r requirements.txt
env $ pip install --editable ./src
```
*Após ativar o ambiente você pode utilizar 'python' e 'pip' sem o 3*

#### anaconda

### Linux

#### virtualenv

#### anaconda