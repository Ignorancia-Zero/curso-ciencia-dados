# Curso de Ciência de Dados

---

## Requisitos
* Python 3.8 64-bit
  * Windows: https://www.python.org/downloads/windows/ ou https://docs.anaconda.com/anaconda/install/windows/
  * Mac: https://www.python.org/downloads/mac-osx/ ou https://docs.anaconda.com/anaconda/install/mac-os/ ou https://formulae.brew.sh/formula/python@3.8
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

## Instalação

### Windows
Explicar processo de criação de ambiente

Explicar problemas de configuração de pacotes
```
cd Suporte/Pacotes Windows
conda update -n base -c defaults conda
conda install -c anaconda beautifulsoup4==4.9.3
conda install -c conda-forge black==21.9b0
pip install charamel==1.0.0
conda install -c anaconda click==7.1.2
conda install -c anaconda jupyter==1.0.0
conda install -c conda-forge jupyter_contrib_nbextensions==0.5.1
conda install -c anaconda lxml==4.6.3
conda install -c anaconda pandas==1.3.3
conda install -c conda-forge patool==1.12
conda install -c conda-forge pyarrow==5.0.0
conda install -c conda-forge pydrive2==1.10.0
pip install pyunpack==0.2.2
conda install -c anaconda yaml==5.4.1
conda install -c anjos rarfile==4.0
conda install -c anaconda requests==2.26.0
conda install -c conda-forge tqdm==4.62.3
pip install ./GDAL-3.3.2-cp38-cp38-win_amd64.whl
pip install ./pyproj-3.2.1-cp38-cp38-win_amd64.whl
pip install ./Fiona-1.8.20-cp38-cp38-win_amd64.whl
pip install ./Shapely-1.7.1-cp38-cp38-win_amd64.whl
pip install ./geopandas-0.9.0-py3-none-any.whl
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