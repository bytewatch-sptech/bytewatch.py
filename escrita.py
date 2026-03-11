import psutil
import csv
import time
import os

arquivoCsv = 'dados.csv'

def criarArquivo():
    if not os.path.exists(arquivoCsv):
        with open('dados.csv', 'w', newline='') as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow(['data', '% Cpu', '% Cpu por nucleo', 'tempo cpu', 'processos ativos', 'ram total', 'ram usada', 'ram disponivel', '% Ram', '% Disco', 'disco disponivel', 'disco usado'])

        print(f'{arquivoCsv} criado com sucesso!')

def capturarDados():
    # Data
    data = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
    
    # Métricas de CPU
    pctCpu = psutil.cpu_percent(interval=1)
    pctPerCpu = psutil.cpu_percent(percpu=True)
    pctPerCpu = '|'.join(map(str, pctPerCpu))
    tempoCpu = psutil.cpu_times()
    processosAtivos = len(psutil.pids())

    # Métricas de RAM
    mem = psutil.virtual_memory()
    totalRam = mem.total
    ramUsada = mem.used
    ramDisponivel = mem.available
    pctRam = mem.percent 

    # Métricas de Disco
    disk = psutil.disk_usage('/')
    pctDisco = disk.percent
    discoDisponivel = disk.free
    discoUsado = disk.used

    return data, pctCpu, pctPerCpu, tempoCpu, processosAtivos, totalRam, ramUsada, ramDisponivel, pctRam, pctDisco, discoDisponivel, discoUsado

def atualizarArquivo():
    criarArquivo()

    while True:
        data, pctCpu, pctPerCpu, tempoCpu, processosAtivos, totalRam, ramUsada, ramDisponivel, pctRam, pctDisco, discoDisponivel, discoUsado = capturarDados()

        with open(arquivoCsv, 'a', newline='') as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow([data, pctCpu, pctPerCpu, tempoCpu, processosAtivos, totalRam, ramUsada, ramDisponivel, pctRam, pctDisco, discoDisponivel, discoUsado])

            print('Atualizando arquivo...')
            time.sleep(1)

atualizarArquivo()