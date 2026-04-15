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
        ultimo = self.dataframe.iloc[-1]

        horas = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        nome_maquina = ultimo["nome_maquina"]
        processador = ultimo["processador"]

        macAddress = ultimo["macAddress"]

        cpuPorcentagem = ultimo["cpuPorcentagem"]
        cpuNucleosFisicos = ultimo["cpuNucleosFisicos"]
        cpuNucleosLogicos = ultimo["cpuNucleosLogicos"]

        cpuTempoUser = round(ultimo["cpuTempoUser"] / 60)
        cpuTempoSistema = round(ultimo["cpuTempoSistema"] / 60)
        cpuTempoInativo = round(ultimo["cpuTempoInativo"] / 60)

        ramLivre  = round(ultimo["ramLivre"]  / 1024**3, 2)
        ramUsada  = round(ultimo["ramUsada"]  / 1024**3, 2)
        ramTotal  = round(ultimo["ramTotal"]  / 1024**3, 2)

        discoLivre = round(ultimo["discoLivre"] / 1024**3, 2)
        discoUsado = round(ultimo["discoUsado"] / 1024**3, 2)
        discoTotal = round(ultimo["discoTotal"] / 1024**3, 2)

        mediaRam   = round(self.dataframe["ramUsada"].mean() / 1024**3, 2)
        porcentagemRam = round((ramUsada / ramTotal) * 100, 2)
        mediaDisco = round(self.dataframe["discoUsado"].mean() / 1024**3, 2)
        porcentagemDisco = round((discoUsado / discoTotal) * 100, 2)

        megabytesEnviados = round(ultimo["bytesEnviados"] / 1024**2, 2)
        megabytesRecebidos = round(ultimo["bytesRecebidos"] / 1024**2, 2)
        velocidadeDownload = round(ultimo["velocidadeDownload"])
        velocidadeUpload = round(ultimo["velocidadeUpload"])

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

        return pd.DataFrame(dados_resultados)
    
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