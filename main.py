from escrita import Escrita
import time

capturaDadosComponentes = Escrita()

while True:
    dadosComponentes = capturaDadosComponentes.obterInformacoesComponentes()
    arquivoMetricas = capturaDadosComponentes.arquivoMetricas
    capturaDadosComponentes.salvarArquivo(dadosComponentes, arquivoMetricas)

    dadosProcessos = capturaDadosComponentes.capturarProcessos()
    arquivoProcessos = capturaDadosComponentes.arquivoProcessos
    capturaDadosComponentes.salvarArquivo(dadosProcessos, arquivoProcessos)

    time.sleep(10)
