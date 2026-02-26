import psutil
import csv
import time
import os

arquivoCsv = 'dados.csv'

def criarArquivo():
    if not os.path.exists(arquivoCsv):
        with open('dados.csv', 'w', newline='') as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow(['data', 'cpu', 'ram', 'disco'])

        print(f'{arquivoCsv} criado com sucesso!')

def capturarDados():
    data = time.strftime('%B %d, %Y %H:%M:%S', time.localtime())
    cpu = psutil.cpu_percent(interval=1)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage('/')

    return data, cpu, mem, disk

def atualizarArquivo():
    criarArquivo()

    while True:
        data, cpu, mem, disk = capturarDados()

        with open(arquivoCsv, 'a', newline='') as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([data, cpu, mem.percent, disk.percent])

            print('Atualizando arquivo...')
            time.sleep(1)

atualizarArquivo()