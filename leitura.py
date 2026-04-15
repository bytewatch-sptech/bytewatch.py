import boto3
import pandas as pd
import io
import urllib.parse
import datetime

class Leitura:
    def __init__(self, event):
        self.s3 = boto3.client('s3')
        
        self.bucket = event['Records'][0]['s3']['bucket']['name']
        
        raw_key = event['Records'][0]['s3']['object']['key']
        self.key = urllib.parse.unquote_plus(raw_key, encoding='utf-8')
        
        print(f"Buscando arquivo: {self.key}")
        
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=self.key)
            self.dataframe = pd.read_csv(response['Body'])
            print("Sucesso: Arquivo carregado no Pandas.")
        except Exception as e:
            print(f"Erro ao acessar S3: {str(e)}")
            raise e

    def salvarArquivo(self, dataframe_final):
        new_key = self.key.replace('raw/', 'trusted/').replace('.csv', '_tratado.csv')
        
        csv_buffer = io.StringIO()
        dataframe_final.to_csv(csv_buffer, index=False)
        
        self.s3.put_object(
            Bucket=self.bucket, 
            Key=new_key, 
            Body=csv_buffer.getvalue()
        )
        return new_key

    def agrupar_processos_por_nome(self):
        df = pd.read_csv(self.arquivoProcessos, on_bad_lines='skip')
        ultima_data = df["Data"].max()
        df_ultimo = df[df["Data"] == ultima_data]
        
        agrupado = {}

        data = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i, linha in df_ultimo.iterrows():
            nome = linha["nomeProcesso"]
            
            if nome not in agrupado:
                agrupado[nome] = {"instancias": 0, "cpu_total": 0, "ram_total": 0, "mac_address": linha["macAddress"], "data": data}
            
            agrupado[nome]["instancias"] += 1
            agrupado[nome]["cpu_total"] += linha["consumoCPUProcesso"]
            agrupado[nome]["ram_total"] += linha["consumoRAMProcesso"]

        dataframeProcessos = pd.DataFrame.from_dict(agrupado, orient="index").reset_index()
        return ultima_data, dataframeProcessos.round(2)

    def formatarDadosComponentes(self):

        horas = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.dataframe["nome_maquina"] = self.dataframe["nome_maquina"]
        self.dataframe["processador"] = self.dataframe["processador"]

        self.dataframe["macAddress"] = self.dataframe["macAddress"]

        self.dataframe["cpuPorcentagem"] = self.dataframe["cpuPorcentagem"]
        self.dataframe["cpuNucleosFisicos"] = self.dataframe["cpuNucleosFisicos"]
        self.dataframe["cpuNucleosLogicos"] = self.dataframe["cpuNucleosLogicos"]

        self.dataframe["cpuTempoUser"] = round(self.dataframe["cpuTempoUser"] / 60)
        self.dataframe["cpuTempoSistema"] = round(self.dataframe["cpuTempoSistema"] / 60)
        self.dataframe["cpuTempoInativo"] = round(self.dataframe["cpuTempoInativo"] / 60)

        self.dataframe["ramLivre"]  = round(self.dataframe["ramLivre"]  / 1024**3, 2)
        self.dataframe["ramUsada"] = round(self.dataframe["ramUsada"]  / 1024**3, 2)
        self.dataframe["ramTotal"]  = round(self.dataframe["ramTotal"]  / 1024**3, 2)

        self.dataframe["discoLivre"] = round(self.dataframe["discoLivre"] / 1024**3, 2)
        self.dataframe["discoUsado"] = round(self.dataframe["discoUsado"] / 1024**3, 2)
        self.dataframe["discoTotal"] = round(self.dataframe["discoTotal"] / 1024**3, 2)

        self.dataframe["mediaRam"] = round(self.dataframe["ramUsada"].mean(), 2)
        self.dataframe["mediaDisco"] = round(self.dataframe["discoUsado"].mean(), 2)
        self.dataframe["percentualRam"] = round((self.dataframe["ramUsada"] / self.dataframe["ramTotal"]) * 100, 2)
        self.dataframe["percentualDisco"] = round((self.dataframe["discoUsado"] / self.dataframe["discoTotal"]) * 100, 2)
        
        self.dataframe["bytesEnviados"] = round(self.dataframe["bytesEnviados"] / 1024**2, 2)
        self.dataframe["bytesRecebidos"] = round(self.dataframe["bytesRecebidos"] / 1024**2, 2)
        self.dataframe["velocidadeDownload"] = round(self.dataframe["velocidadeDownload"])
        self.dataframe["velocidadeUpload"] = round(self.dataframe["velocidadeUpload"])

        return self.dataframe
    
    def mainLoop(self):
        dados = self.formatarDadosComponentes()
        # self.salvarArquivo(dados, "leituraTratada.csv")
        # ultima_data_processos, processos_agrupados = self.agrupar_processos_por_nome()
        # self.salvarArquivo(processos_agrupados, "dadosProcessoTratado.csv")

def lambda_handler(event, context):
    try:
        processador = Leitura(event)
        df_final = processador.formatarDadosComponentes()
        caminho_salvo = processador.salvarArquivo(df_final)
        
        return {
            'statusCode': 200,
            'body': f'Sucesso! Arquivo salvo em: {caminho_salvo}'
        }
    except Exception as e:
        print(f"Erro: {str(e)}")
        raise e