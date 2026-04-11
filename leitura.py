import time, datetime, os, uuid
from re import findall
import pandas as pd

class Leitura:
    ultimo_horario = None
    ultimo_dado = None
    dataframe = None
    nomeArquivo = ""
    arquivoRaw = ""
    tipoArquivo = ""
    arquivoProcessos = ""

    def __init__(self):
        self.nomeArquivo = f"metricas_{self.obterMacAddress()}_raw.csv"
        self.arquivoProcessos = f"processos_{self.obterMacAddress()}_raw.csv"

    def obterMacAddress(self):
        mac = ':'.join(findall('..', '%012x' % uuid.getnode()))
        print(f"MAC Address: {mac}")
        return mac

    def salvarArquivo(self, dado, nomeArquivo):
        dataframe = pd.DataFrame(dado)
        if not os.path.exists(nomeArquivo):
            dataframe.to_csv(nomeArquivo, index=False)
        else:
            dataframe.to_csv(nomeArquivo, mode="a", header=False, index=False)

    def agrupar_processos_por_nome(self):
        df = pd.read_csv(self.arquivoProcessos, on_bad_lines='skip')
        ultima_data = df["Data"].max()
        df_ultimo = df[df["Data"] == ultima_data]
        
        agrupado = {}

        data = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i, linha in df_ultimo.iterrows():
            nome = linha["nomeProcesso"]
            
            # Se o processo ainda não está no dicionário, cria com valores zerados
            if nome not in agrupado:
                agrupado[nome] = {"instancias": 0, "cpu_total": 0, "ram_total": 0, "mac_address": linha["macAddress"], "data": data}
            
            # Soma os valores
            agrupado[nome]["instancias"] += 1
            agrupado[nome]["cpu_total"] += linha["consumoCPUProcesso"]
            agrupado[nome]["ram_total"] += linha["consumoRAMProcesso"]

        # Converte o dicionário para DataFrame e arredonda
        dataframeProcessos = pd.DataFrame.from_dict(agrupado, orient="index").reset_index()
        return ultima_data, dataframeProcessos.round(2)

    def buscarUltimasInformacoes(self, arquivo):
        df = pd.read_csv(arquivo, on_bad_lines='skip')
        self.dataframe = df

        ultimo = df.iloc[-1]

        if ultimo["horario"] == self.ultimo_horario:
            print(f"sem novos dados desde {self.ultimo_horario}, aguardando próxima leitura...")
            return False
        else:
            self.ultimo_horario = ultimo["horario"]
            self.ultimo_dado = ultimo
            return True
        
    def formatarDadosComponentes(self):
        horas = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nome_maquina = self.ultimo_dado["nome_maquina"]
        processador = self.ultimo_dado["processador"]

        macAddress = self.ultimo_dado["macAddress"]

        cpuPorcentagem = self.ultimo_dado["cpuPorcentagem"]
        cpuNucleosFisicos = self.ultimo_dado["cpuNucleosFisicos"]
        cpuNucleosLogicos = self.ultimo_dado["cpuNucleosLogicos"]

        cpuTempoUser = round(self.ultimo_dado["cpuTempoUser"] / 60)
        cpuTempoSistema = round(self.ultimo_dado["cpuTempoSistema"] / 60)
        cpuTempoInativo = round(self.ultimo_dado["cpuTempoInativo"] / 60)

        ramLivre  = round(self.ultimo_dado["ramLivre"]  / 1024**3, 2)
        ramUsada  = round(self.ultimo_dado["ramUsada"]  / 1024**3, 2)
        ramTotal  = round(self.ultimo_dado["ramTotal"]  / 1024**3, 2)

        discoLivre = round(self.ultimo_dado["discoLivre"] / 1024**3, 2)
        discoUsado = round(self.ultimo_dado["discoUsado"] / 1024**3, 2)
        discoTotal = round(self.ultimo_dado["discoTotal"] / 1024**3, 2)

        mediaRam   = round(self.dataframe["ramUsada"].mean() / 1024**3, 2)
        porcentagemRam = round((ramUsada / ramTotal) * 100, 2)
        mediaDisco = round(self.dataframe["discoUsado"].mean() / 1024**3, 2)
        porcentagemDisco = round((discoUsado / discoTotal) * 100, 2)

        megabytesEnviados = round(self.ultimo_dado["bytesEnviados"] / 1024**2, 2)
        megabytesRecebidos = round(self.ultimo_dado["bytesRecebidos"] / 1024**2, 2)
        velocidadeDownload = round(self.ultimo_dado["velocidadeDownload"])
        velocidadeUpload = round(self.ultimo_dado["velocidadeUpload"])

        dados_resultados = {
            "Horário": [horas],
            "macAddress": [macAddress],
            "nome_maquina": [nome_maquina],
            "processador": [processador],
            "cpuPorcentagem": [cpuPorcentagem],
            "cpuNucleosFisicos": [cpuNucleosFisicos],
            "cpuNucleosLogicos": [cpuNucleosLogicos],
            "cpuTempoUser": [cpuTempoUser],
            "cpuTempoSistema": [cpuTempoSistema],
            "cpuTempoInativo": [cpuTempoInativo],
            "ramLivre": [ramLivre],
            "ramUsada": [ramUsada],
            "ramTotal": [ramTotal],
            "discoLivre": [discoLivre],
            "discoUsado": [discoUsado],
            "discoTotal": [discoTotal],
            "mediaRamGB": [mediaRam],
            "mediaDiscoGB": [mediaDisco],
            "porcentagemRam": [porcentagemRam],
            "porcentagemDisco": [porcentagemDisco],
            "megabytesEnviados": [megabytesEnviados],
            "megabytesRecebidos": [megabytesRecebidos],
            "velocidadeDownload": [velocidadeDownload],
            "velocidadeUpload": [velocidadeUpload]
        }

        return dados_resultados
    
    def mainLoop(self):
        self.buscarUltimasInformacoes(self.nomeArquivo)
        dados = self.formatarDadosComponentes()
        self.salvarArquivo(dados, "leituraTratada.csv")
        ultima_data_processos, processos_agrupados = self.agrupar_processos_por_nome()
        self.salvarArquivo(processos_agrupados, "dadosProcessoTratado.csv")

