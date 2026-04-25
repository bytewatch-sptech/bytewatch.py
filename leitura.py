import time, datetime, os, uuid, glob, subprocess, boto3
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
        self.bucket = "bytewatch-sptech"

    def sync_to_s3(self, bucket_name, path, local_path="./"):
        command = f"aws s3 sync s3://{bucket_name}/{path} {local_path}"
        try:
            subprocess.run(command, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Erro na sincronização: {e}")

    def salvarArquivoNoBucket(self, file, bucket, path, fileName):
        s3 = boto3.client('s3')

        s3.upload_file(
            Filename=f'{file}',
            Bucket=f'{bucket}',    
            Key=f'{path}/{fileName}'
        )

    def agrupar_dados_csv(self, tipo_arquivo, arquivo_final):
        todos_dataframes = []
        if tipo_arquivo == "metricas":
            caminho_arquivos = os.path.join("./", "metricas_*_raw.csv")
        elif tipo_arquivo == "processos":
            caminho_arquivos = os.path.join("./", "processos_*_raw.csv")
        arquivos = glob.glob(caminho_arquivos)

        if not arquivos:
            print("Nenhum arquivo")
            return

        for arquivo in arquivos:
            df_temp = pd.read_csv(arquivo, on_bad_lines='skip')
            self.dataframe = df_temp 
            
            if tipo_arquivo == "metricas":
                for _, linha in df_temp.iterrows():
                    self.ultimo_dado = linha
                    
                    dados_formatados = self.formatarDadosComponentes()
                    
                    dados_limpos = {k: v[0] for k, v in dados_formatados.items()}
                    todos_dataframes.append(dados_limpos)

            elif tipo_arquivo == "processos":
                dados_formatados = self.agrupar_processos_por_nome()
                todos_dataframes.extend(dados_formatados.to_dict(orient='records'))

        if todos_dataframes:
            df_trusted = pd.DataFrame(todos_dataframes)
            
            df_trusted.to_csv(arquivo_final, index=False, encoding='utf-8')

    def obterMacAddress(self):
        mac = ':'.join(findall('..', '%012x' % uuid.getnode()))
        print(f"MAC Address: {mac}")
        return mac

    def agrupar_processos_por_nome(self):
        agrupado = {}

        for i, linha in self.dataframe.iterrows():
            nome = linha["nomeProcesso"]
            dataProcesso = linha["Data"]
            chave = (nome, dataProcesso)

            if chave not in agrupado:
                agrupado[chave] = {"nome_processo": linha["nomeProcesso"], "instancias": 0, "cpu_total": 0, "ram_total": 0, "mac_address": linha["macAddress"], "data": dataProcesso}

            agrupado[chave]["instancias"] += 1
            agrupado[chave]["cpu_total"] += linha["consumoCPUProcesso"]
            agrupado[chave]["ram_total"] += linha["consumoRAMProcesso"]

        dataframeProcessos = pd.DataFrame.from_dict(agrupado, orient="index").reset_index()
        dataframeProcessos = dataframeProcessos.drop(['level_0', 'level_1'], axis=1)
        # dataframeProcessos = dataframeProcessos.sort_values(by="ram_total", ascending=False)
        return dataframeProcessos.round(2)
        
    def formatarDadosComponentes(self):
        horas = self.ultimo_dado["horario"]
        nome_maquina = self.ultimo_dado["nome_maquina"]
        processador = self.ultimo_dado["processador"]

        droppedPackets = self.ultimo_dado["droppedPackets"]
        conexoesAtivas = self.ultimo_dado["conexoesAtivas"]

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
        velocidadeDownload = round(self.ultimo_dado["velocidadeDownload"] / 1024**2, 2)
        velocidadeUpload = round(self.ultimo_dado["velocidadeUpload"] / 1024**2, 2)

        dados_resultados = {"horario": [horas], "macAddress": [macAddress], "nome_maquina": [nome_maquina], "processador": [processador], "cpuPorcentagem": [cpuPorcentagem], "cpuNucleosFisicos": [cpuNucleosFisicos], "cpuNucleosLogicos": [cpuNucleosLogicos], "cpuTempoUser": [cpuTempoUser], "cpuTempoSistema": [cpuTempoSistema], "cpuTempoInativo": [cpuTempoInativo], "ramLivre": [ramLivre], "ramUsada": [ramUsada], "ramTotal": [ramTotal], "discoLivre": [discoLivre], "discoUsado": [discoUsado], "discoTotal": [discoTotal], "mediaRamGB": [mediaRam], "mediaDiscoGB": [mediaDisco], "porcentagemRam": [porcentagemRam], "porcentagemDisco": [porcentagemDisco], "megabytesEnviados": [megabytesEnviados], "megabytesRecebidos": [megabytesRecebidos], "velocidadeDownload": [velocidadeDownload], "velocidadeUpload": [velocidadeUpload], "droppedPackets": [droppedPackets], "conexoesAtivas": [conexoesAtivas]}

        return dados_resultados
    
    def mainLoop(self):
        self.sync_to_s3(self.bucket, "raw")
        self.agrupar_dados_csv("metricas", "metricas_trusted.csv")
        self.agrupar_dados_csv("processos", "processos_trusted.csv")
        self.salvarArquivoNoBucket("processos_trusted.csv", self.bucket, "trusted", "processos_trusted.csv")
        self.salvarArquivoNoBucket("metricas_trusted.csv", self.bucket, "trusted", "metricas_trusted.csv")
        

