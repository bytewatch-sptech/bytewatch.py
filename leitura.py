import csv
from datetime import datetime
import time

data = []
cpu = []
cpuNucleo = []
processos = []
ram = []
ramTotal = []
ramUsada = []
ramDisponivel = []
disco = []
discoUsado = []
discoDisponivel = []

def lerDados():
    data.clear()
    cpu.clear()
    cpuNucleo.clear()
    processos.clear()
    ram.clear()
    ramTotal.clear()
    ramUsada.clear()
    ramDisponivel.clear()
    disco.clear()
    discoUsado.clear()
    discoDisponivel.clear()

    with open('dados.csv', mode='r') as csvfile:
        leitor = csv.DictReader(csvfile)

        for linha in leitor:
            data.append(datetime.strptime(linha["Data"], '%Y-%m-%d %H:%M:%S'))
            cpu.append(float(linha["% CPU"]))
            cpuNucleo.append(linha["% CPU por nucleo"])
            processos.append(float(linha["Processos ativos"]))
            ramTotal.append(float(linha["RAM total"]))
            ramUsada.append(float(linha["RAM usada"]))
            ramDisponivel.append(float(linha["RAM disponivel"]))
            ram.append(float(linha["% RAM"]))
            disco.append(float(linha["% Disco"]))
            discoUsado.append(float(linha["Disco usado"]))
            discoDisponivel.append(float(linha["Disco disponivel"]))

    mediaCpu = sum(cpu) / len(cpu)
    picoCpu = max(cpu)
    mediaRam = sum(ram) / len(ram)
    picoRam = max(ram)
    mediaDisco = sum(disco) / len(disco)
    picoDisco = max(disco)
    mediaProcessos = sum(processos) / len(processos)
    tempoMonitorado = data[-1] - data[0]
    capturasTotal = len(data)

    with open('relatorio.csv', mode='w', newline='') as csvfile:
        escritor = csv.writer(csvfile)
        escritor.writerow(["Media CPU", round(mediaCpu, 1)])
        escritor.writerow(["Pico CPU", round(picoCpu, 1)])
        escritor.writerow(["Media RAM", round(mediaRam, 1)])
        escritor.writerow(["Pico RAM", round(picoRam, 1)])
        escritor.writerow(["Media Disco", round(mediaDisco, 1)])
        escritor.writerow(["Pico Disco", round(picoDisco, 1)])
        escritor.writerow(["Media Processos", round(mediaProcessos, 1)])
        escritor.writerow(["Capturas Totais", (capturasTotal, 1)])
        escritor.writerow(["Tempo Monitorado", (tempoMonitorado, 1)])

        print(f"Média CPU: {mediaCpu:.1f}%")
        print(f"Pico CPU: {picoCpu:.1f}%")
        print(f"Média RAM: {mediaRam:.1f}%")
        print(f"Pico RAM: {picoRam:.1f}%")
        print(f"Média Disco: {mediaDisco:.1f}%")
        print(f"Pico Disco: {picoDisco:.1f}%")
        print(f"Média Processos: {mediaProcessos:.0f}")
        print(f"Capturas Totais: {capturasTotal}")
        print(f"Tempo Monitorado: {tempoMonitorado}\n")

lerDados()