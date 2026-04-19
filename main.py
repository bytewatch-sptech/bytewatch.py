# pip install -r requirements.txt

from escrita import Escrita
from leitura import Leitura
from db import database
import time

capturaDadosComponentes = Escrita()
leituraDadosComponentes = Leitura()

macAddress = capturaDadosComponentes.macAddress
if (not database.macAddressExiste(macAddress)):
    print(f"Servidor com mac address {macAddress} não está cadastrado!")

else:
    while True:
        dadosComponentes = capturaDadosComponentes.obterInformacoesComponentes()
        arquivoMetricas = capturaDadosComponentes.arquivoMetricas
        capturaDadosComponentes.salvarArquivo(dadosComponentes, arquivoMetricas)
        capturaDadosComponentes.salvarArquivoNoBucket(arquivoMetricas, "bytewatch-sptech", "raw", arquivoMetricas)

        dadosProcessos = capturaDadosComponentes.capturarProcessos()
        arquivoProcessos = capturaDadosComponentes.arquivoProcessos
        capturaDadosComponentes.salvarArquivo(dadosProcessos, arquivoProcessos)
        capturaDadosComponentes.salvarArquivoNoBucket(arquivoProcessos, "bytewatch-sptech", "raw", arquivoProcessos)


        leituraDadosComponentes.mainLoop()

        time.sleep(10)
