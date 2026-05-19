import time
from escrita import Escrita
from leitura import Leitura
from db import database

from client import Client

capturaDadosComponentes = Escrita()
leituraDadosComponentes = Leitura()

macAddress = capturaDadosComponentes.macAddress

while True:
    dadosComponentes = capturaDadosComponentes.obterInformacoesComponentes()
    arquivoMetricas = capturaDadosComponentes.arquivoMetricas
    capturaDadosComponentes.salvarArquivo(dadosComponentes, arquivoMetricas)
    capturaDadosComponentes.salvarArquivoNoBucket(
        arquivoMetricas, "amzn-s3-bucket-teste-projeto", "raw", arquivoMetricas
    )

    dadosProcessos = capturaDadosComponentes.capturarProcessos()

    arquivoProcessos = getattr(
        capturaDadosComponentes, "arquivoProcessos", "processos.json"
    )

    capturaDadosComponentes.salvarArquivo(dadosProcessos, arquivoProcessos)
    capturaDadosComponentes.salvarArquivoNoBucket(
        arquivoProcessos, "amzn-s3-bucket-teste-projeto", "raw", arquivoProcessos
    )

    leituraDadosComponentes.mainLoop()

    try:
        processador = Client()
        processador.tratarMetricas()
        processador.tratarProcessos()
        processador.salvarArquivo()

        capturaDadosComponentes.salvarArquivoNoBucket(
            "client.json", "amzn-s3-bucket-teste-projeto", "trusted", "client.json"
        )
        print("client.json atualizado e enviado para o S3 com sucesso.")
    except Exception as e:
        print(f"Erro ao processar client.json: {e}")

    time.sleep(10)
