# pip install -r requirements.txt

from escrita import Escrita
from leitura import Leitura
from db import database
import time

capturaDadosComponentes = Escrita()
leituraDadosComponentes = Leitura()

while True:
    macAddress = capturaDadosComponentes.macAddress
    if (not database.macAddressExiste(macAddress)):
        print(f"Servidor com mac address {macAddress} não está cadastrado!")
        break

    dadosComponentes = capturaDadosComponentes.obterInformacoesComponentes()
    arquivoMetricas = capturaDadosComponentes.arquivoMetricas
    capturaDadosComponentes.salvarArquivo(dadosComponentes, arquivoMetricas)

    dadosProcessos = capturaDadosComponentes.capturarProcessos()
    arquivoProcessos = capturaDadosComponentes.arquivoProcessos
    capturaDadosComponentes.salvarArquivo(dadosProcessos, arquivoProcessos)

    leituraDadosComponentes.mainLoop()

    time.sleep(10)
