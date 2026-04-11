from escrita import Escrita
from leitura import Leitura
import time

capturaDadosComponentes = Escrita()
leituraDadosComponentes = Leitura()

while True:
    dadosComponentes = capturaDadosComponentes.obterInformacoesComponentes()
    arquivoMetricas = capturaDadosComponentes.arquivoMetricas
    capturaDadosComponentes.salvarArquivo(dadosComponentes, arquivoMetricas)

    dadosProcessos = capturaDadosComponentes.capturarProcessos()
    arquivoProcessos = capturaDadosComponentes.arquivoProcessos
    capturaDadosComponentes.salvarArquivo(dadosProcessos, arquivoProcessos)

    leituraDadosComponentes.mainLoop()

    time.sleep(10)
