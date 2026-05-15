import time, datetime, os, uuid, glob, subprocess, boto3
from re import findall
import pandas as pd
import io 
import urllib.parse 

class Leitura:
    def __init__(self, event): 
        self.bucket = event['Records'][0]['s3']['bucket']['name'] 
        raw_key = event['Records'][0]['s3']['object']['key'] 
        self.arquivoRaw = urllib.parse.unquote_plus(raw_key, encoding='utf-8') 
        self.nomeArquivo = os.path.basename(self.arquivoRaw) 

        s3 = boto3.client('s3')
        try:
            response = s3.get_object(Bucket=self.bucket, Key=self.arquivoRaw) 
            self.dataframe = pd.read_csv(response['Body'], on_bad_lines='skip') 
        except Exception as e:
            self.dataframe = None
            print(f"Erro na leitura do S3: {e}")

    def salvarArquivoNoBucket(self, dataframe_final, bucket, path, fileName): 
        s3 = boto3.client('s3')

        if "metricas" in fileName:
            dataframe_final = dataframe_final.drop_duplicates(subset=['horario', 'macAddress'], keep='last')
        else:
            dataframe_final = dataframe_final.drop_duplicates(subset=['nome_processo', 'data', 'mac_address'], keep='last')

        csv_buffer = io.StringIO()
        dataframe_final.to_csv(csv_buffer, index=False, encoding='utf-8')

        s3.put_object( 
            Bucket=f'{bucket}',    
            Key=f'{path}/{fileName}',
            Body=csv_buffer.getvalue() 
        )
        

    def agrupar_dados_csv(self, tipo_arquivo, arquivo_final):
        todos_dataframes = []
        
        if self.dataframe is None: 
            print("Nenhum arquivo para processar")
            return None 

        df_temp = self.dataframe 
        
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
            df_novo = pd.DataFrame(todos_dataframes) 
            
            s3 = boto3.client('s3')
            try:
                response_trusted = s3.get_object(Bucket=self.bucket, Key=f"trusted/{arquivo_final}")
                df_trusted_existente = pd.read_csv(response_trusted['Body'], on_bad_lines='skip')
                
                df_final = pd.concat([df_trusted_existente, df_novo], ignore_index=True) 
            except s3.exceptions.NoSuchKey:
                df_final = df_novo 

            return df_final 

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
        dataframeProcessos = dataframeProcessos.drop(['level_0', 'level_1'], axis=1, errors='ignore')
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

        velocidadeEscrita = round(self.ultimo_dado["velocidadeEscrita"] / 1024**2, 2)
        velocidadeLeitura = round(self.ultimo_dado["velocidadeLeitura"] / 1024**2, 2)

        dados_resultados = {
            "horario": [horas], "macAddress": [macAddress], "nome_maquina": [nome_maquina], 
            "processador": [processador], "cpuPorcentagem": [cpuPorcentagem], "cpuNucleosFisicos": [cpuNucleosFisicos], 
            "cpuNucleosLogicos": [cpuNucleosLogicos], "cpuTempoUser": [cpuTempoUser], "cpuTempoSistema": [cpuTempoSistema], 
            "cpuTempoInativo": [cpuTempoInativo], "ramLivre": [ramLivre], "ramUsada": [ramUsada], 
            "ramTotal": [ramTotal], "discoLivre": [discoLivre], "discoUsado": [discoUsado], 
            "discoTotal": [discoTotal], "velocidadeEscrita": [velocidadeEscrita], "velocidadeLeitura": [velocidadeLeitura], 
            "mediaRamGB": [mediaRam], "mediaDiscoGB": [mediaDisco], "porcentagemRam": [porcentagemRam], 
            "porcentagemDisco": [porcentagemDisco], "megabytesEnviados": [megabytesEnviados], 
            "megabytesRecebidos": [megabytesRecebidos], "velocidadeDownload": [velocidadeDownload], 
            "velocidadeUpload": [velocidadeUpload], "droppedPackets": [droppedPackets], "conexoesAtivas": [conexoesAtivas]
        }

        return dados_resultados
    
    def mainLoop(self):
        if self.dataframe is None:
            return

        if "metricas" in self.nomeArquivo:
            df_final = self.agrupar_dados_csv("metricas", "metricas_trusted.csv")
            if df_final is not None:
                self.salvarArquivoNoBucket(df_final, self.bucket, "trusted", "metricas_trusted.csv") 
                
        elif "processos" in self.nomeArquivo:
            df_final = self.agrupar_dados_csv("processos", "processos_trusted.csv")
            if df_final is not None:
                self.salvarArquivoNoBucket(df_final, self.bucket, "trusted", "processos_trusted.csv") 

def lambda_handler(event, context):
    try:
        app = Leitura(event)
        app.mainLoop()
        return {
            'statusCode': 200, 
            'body': 'Arquivo processado, append e deduplicação realizados com sucesso.'
        }
    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
        raise e