import argparse

parser = argparse.ArgumentParser(
                    prog='SM Teste PA',
                    description='Baixa os arquivos de disseminação de procedimentos ambulatorias do FTP do DataSUS'
)

parser.add_argument('-u', '--UF')
parser.add_argument('-d', '--data')
parser.add_argument('-p', '--path')

x = parser.parse_args()

print(x.UF)
