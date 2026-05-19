import boto3
import pandas as pd
import json
import urllib.parse

class Client:
    def __init__(self, event):
        self.s3 = boto3.client('s3')
        self.bucket = event['Records'][0]['s3']['bucket']['name']
        
        self.conteudo = {}
        
        self.df_metrica = self.buscarArquivoNoS3("trusted/metricas_trusted.csv")
        self.df_processos = self.buscarArquivoNoS3("trusted/processos_trusted.csv")

    def buscarArquivoNoS3(self, key):
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            return pd.read_csv(response['Body'], on_bad_lines='skip')
        except Exception as e:
            return pd.DataFrame()

    def salvarArquivo(self, nomeArquivo):
        output_key = f"client/{nomeArquivo}"
        
        json_data = json.dumps(self.conteudo, indent=4, ensure_ascii=False)
        
        self.s3.put_object(
            Bucket=self.bucket,
            Key=output_key,
            Body=json_data,
            ContentType='application/json'
        )

    def dashboardAlertas(self):
        self.salvarArquivo("dashboard_alertas.json")

    def dashboardGestor(self):
        self.salvarArquivo("dashboard_gestor.json")

    def dashboardServidoresGerais(self):
        if self.df_metrica.empty:
            return
        
        def definir_status_ui(porcentagem):
            if porcentagem >= 90:
                return "critico"
            elif porcentagem >= 70:
                return "risco"
            return "estavel"
        
            lista_servidores_json = []
            total_criticos = 0
        
        for mac in self.df_metrica["macAddress"].unique():
            df_maquina = self.df_metrica[self.df_metrica["macAddress"] == mac].sort_values(by="horario")
            ultima_linha = df_maquina.iloc[-1]

            cpu_val = float(ultima_linha.cpu def dashboardServidoresGerais(self):
        if self.df_metrica.empty:
            return
        
        def definir_status_ui(porcentagem):
            if porcentagem >= 90:
                return "critico"
            elif porcentagem >= 70:
                return "risco"
            return "estavel"
        
        lista_servidores_json = []
        total_critico = 0
        
        for mac in self.df_metrica["macAddress"].unique():
            df_maquina = self.df_metrica[self.df_metrica["macAddress"] == mac].sort_values(by="horario")
            ultima_linha = df_maquina.iloc[-1]

            cpu_val = float(ultima_linha.cpuPorcentagem)
            ram_val = float(ultima_linha.porcentagemRam)
            disco_val = float(ultima_linha.porcentagemDisco)

            vel_upload = float(ultima_linha.velocidadeUpload)
            vel_download = float(ultima_linha.velocidadeDownload)
            dropped = int(ultima_linha.droppedPackets)
            
            capacidade_nominal_MB = 125.0

            saturacao_rede = ((vel_upload + vel_download)/ capacidade_nominal_MB) * 100
            p_saturacao = (saturacao_rede - 70) * 1.5 if saturacao_rede > 70 else 0.0
            p_erro = dropped * 2.0
            rede_eficiencia = max(0.0, min(100.0, 100.0 - p_saturacao - p_erro))

            status_rede = "critico" if rede_eficiencia < 70 else ("atencao" if rede_eficiencia < 85 else "estavel")
            
            status_cpu = definir_status_ui(cpu_val)
            status_ram = definir_status_ui(ram_val)
            status_disco = definir_status_ui(disco_val)

            #Status do servidor
            status_componentes = [status_cpu, status_ram, status_disco]
            if "critico" in status_componentes or status_componentes.count("risco") >= 2:
                status_servidor = "critico"
                total_criticos += 1
            elif status_componentes.count("risco") == 1:
                status_servidor = "risco"
            else:
                status_servidor = "estavel"

            #Diagnósticos escritos:
            gatilhos_diagnostio = []
            if disco_val >= 90: gatilhos_diagnostio.append("Disco Saturado")
            if cpu_val >= 90: gatilhos_diagnostio.append("CPU Saturada")
            if ram_val >= 90: gatilhos_diagnostio.append("RAM Saturada")
            if saturacao_rede >= 75: gatilhos_diagnostio.append("Rede Lenta")

            texto_diagnostico = " + ".join(gatilhos_diagnostio) if gatilhos_diagnostio else "Operação Normal"

            nome_maquina = str(ultima_linha.nome_maquina)

            servidor_objeto = {
                "macAddress": mac,
                "nome": nome_maquina,
                "regiao": "Teste Local Daora",
                "ultimaColeta": str(ultima_linha.horario),
                "status_servidor": status_servidor,
                "diagnostico_etl": texto_diagnostico,
                "geolocalizacao": {
                    "coords": [-23.5505, -46.6333],
                    "saturacao_trafego": round(saturacao_rede, 1)
                },
                "componentes": {
                    "cpu": {
                        "valor": round(cpu_val, 1),
                        "status_ui": status_cpu,
                        "acao_permitida": "abrir"
                    },
                    "ram": {
                        "valor": round(ram_val, 1),
                        "status_ui": status_ram,
                        "acao_permitida": "abrir"
                    },
                    "disco": {
                        "valor": round(disco_val, 1),
                        "status_ui": status_disco,
                        "acao_permitida": "informativo"
                    },
                    "rede": {
                        "valor": round(rede_eficiencia, 1),
                        "status_ui": status_rede,
                        "acao_permitida": "informativo"
                    }
                }
            }
            lista_servidores_json.append(servidor_objeto)
        
        #Ordenação pra mostrar os criticos primeiros
        ordem_status = {"critico": 0, "risco": 1, "estavel": 2}
        lista_servidores_json.sort(key=lambda s: ordem_status[s["status_servidores"]])

        timestamps_unicos = df_ordenado_tempo["horario"].unique()[-15:]

        self.conteudo = {
            "resumo_global":{
                "servidores_criticos": total_criticos,
                "total_servidores": len(lista_servidores_json)
            },
            "frota_servidores": lista_servidores_json
        }

        self.salvarArquivo("dashboard_geral.json")
       Porcentagem)
            ram_val = float(ultima_linha.porcentagemRam)
            disco_val = float(ultima_linha.porcentagemDisco)

            vel_upload = float(ultima_linha.velocidadeUpload)
            vel_download = float(ultima_linha.velocidadeDownload)
            dropped = int(ultima_linha.droppedPackets)
            
            capacidade_nominal_MB = 125.0

            saturacao_rede = ((vel_upload + vel_download)/ capacidade_nominal_MB) * 100
            p_saturacao = (saturacao_rede - 70) * 1.5 if saturacao_rede > 70 else 0.0
            p_erro = dropped * 2.0
            rede_eficiencia = max(0.0, min(100.0, 100.0 - p_saturacao - p_erro))

            status_rede = "critico" if rede_eficiencia < 70 else ("risco" if rede_eficiencia < 85 else "estavel")
            
            status_cpu = definir_status_ui(cpu_val)
            status_ram = definir_status_ui(ram_val)
            status_disco = definir_status_ui(disco_val)

            #Status do servidor
            status_componentes = [status_cpu, status_ram, status_disco]
            if "critico" in status_componentes or status_componentes.count("risco") >= 2:
                status_servidor = "critico"
                total_criticos += 1
            elif status_componentes.count("risco") == 1:
                status_servidor = "risco"
            else:
                status_servidor = "estavel"

            #Diagnósticos escritos:
            gatilhos_diagnostio = []
            if disco_val >= 90: gatilhos_diagnostio.append("Disco Saturado")
            if cpu_val >= 90: gatilhos_diagnostio.append("CPU Saturada")
            if ram_val >= 90: gatilhos_diagnostio.append("RAM Saturada")
            if saturacao_rede >= 75: gatilhos_diagnostio.append("Rede Lenta")

            texto_diagnostico = " + ".join(gatilhos_diagnostio) if gatilhos_diagnostio else "Operação Normal"

            nome_maquina = str(ultima_linha.nome_maquina)

            servidor_objeto = {
                "macAddress": mac,
                "nome": nome_maquina,
                "regiao": "Teste Local Daora",
                "ultimaColeta": str(ultima_linha.horario),
                "status_servidor": status_servidor,
                "diagnostico_etl": texto_diagnostico,
                "geolocalizacao": {
                    "coords": [-23.5505, -46.6333],
                    "saturacao_trafego": round(saturacao_rede, 1)
                },
                "componentes": {
                    "cpu": {
                        "valor": round(cpu_val, 1),
                        "status_ui": status_cpu,
                        "acao_permitida": "abrir"
                    },
                    "ram": {
                        "valor": round(ram_val, 1),
                        "status_ui": status_ram,
                        "acao_permitida": "abrir"
                    },
                    "disco": {
                        "valor": round(disco_val, 1),
                        "status_ui": status_disco,
                        "acao_permitida": "informativo"
                    },
                    "rede": {
                        "valor": round(rede_eficiencia, 1),
                        "status_ui": status_rede,
                        "acao_permitida": "informativo"
                    }
                }
            }
            lista_servidores_json.append(servidor_objeto)
        
        #Ordenação pra mostrar os criticos primeiros
        ordem_status = {"critico": 0, "risco": 1, "estavel": 2}
        lista_servidores_json.sort(key=lambda s: ordem_status[s["status_servidor"]])

        self.conteudo = {
            "resumo_global":{
                "servidores_criticos": total_criticos,
                "total_servidores": len(lista_servidores_json)
            },
            "frota_servidores": lista_servidores_json
        }

        self.salvarArquivo("dashboard_geral.json")
        

    def dashboardServidorEspecifico(self):
        if self.df_metrica.empty:
            return

        for mac in self.df_metrica["macAddress"].unique():
            df_maquina = self.df_metrica[self.df_metrica["macAddress"] == mac]
            ultima_linha = df_maquina.iloc[-1]
            
            if mac not in self.conteudo:
                self.conteudo[mac] = {
                    "metricas": [],
                    "processos": []
                }
            
            self.conteudo[mac]["metricas"].append({
                "tipoDado": "ram",
                "macAddress": mac,
                "ultimaColeta": ultima_linha.horario,
                "porcentagemRam": ultima_linha.porcentagemRam,
                "ramTotal": ultima_linha.ramTotal,
                "ramUsada": ultima_linha.ramUsada,
                "kpi": {
                    "percentualUsado": ultima_linha.porcentagemRam,
                    "percentualLivre": 100 - ultima_linha.porcentagemRam
                },
                "grafico": {
                    "percentualUsado": df_maquina["porcentagemRam"].tolist(),
                    "momento": df_maquina["horario"].tolist(),
                }
            })

            self.conteudo[mac]["metricas"].append({
                "tipoDado": "disco",
                "macAddress": mac,
                "ultimaColeta": ultima_linha.horario,
                "porcentagemDisco": ultima_linha.porcentagemDisco,
                "discoTotal": ultima_linha.discoTotal,
                "discoUsado": ultima_linha.discoUsado,
                "velocidadeLeitura": int(ultima_linha.velocidadeLeitura),
                "velocidadeEscrita": int(ultima_linha.velocidadeEscrita),
                "kpi": {
                    "percentualUsado": ultima_linha.porcentagemDisco,
                    "percentualLivre": 100 - ultima_linha.porcentagemDisco
                },
                "grafico": {
                    "percentualUsado": df_maquina["porcentagemDisco"].tolist(),
                    "velocidadeEscrita": df_maquina["velocidadeEscrita"].tolist(),
                    "velocidadeLeitura": df_maquina["velocidadeLeitura"].tolist(),
                    "momento": df_maquina["horario"].tolist(),
                }
            })

            self.conteudo[mac]["metricas"].append({
                "tipoDado": "cpu",
                "macAddress": mac,
                "ultimaColeta": ultima_linha.horario,
                "processador": ultima_linha.processador,
                "porcentagemCpu": ultima_linha.cpuPorcentagem,
                "coresLogicos": int(ultima_linha.cpuNucleosLogicos),
                "kpi": {
                    "percentualUsado": ultima_linha.cpuPorcentagem,
                    "percentualLivre": 100 - ultima_linha.cpuPorcentagem
                },
                "grafico": {
                    "percentualUsado": df_maquina["cpuPorcentagem"].tolist(),
                    "momento": df_maquina["horario"].tolist(),
                }
            })

            self.conteudo[mac]["metricas"].append({
                "tipoDado": "rede",
                "macAddress": mac,
                "ultimaColeta": ultima_linha.horario,
                "totalMegabytesEnviados": ultima_linha.megabytesEnviados,
                "totalMegabytesRecebidos": ultima_linha.megabytesRecebidos,
                "megabytesEnviados": ultima_linha.velocidadeUpload,
                "megabytesRecebidos": ultima_linha.velocidadeDownload,
                "droppedPackets": int(ultima_linha.droppedPackets),
                "conexoesAtivas": int(ultima_linha.conexoesAtivas),
                "kpi": {},
                "grafico": {
                    "megabytesEnviados": df_maquina["velocidadeUpload"].tolist(),
                    "megabytesRecebidos": df_maquina["velocidadeDownload"].tolist(),
                    "momento": df_maquina["horario"].tolist(),
                }
            })
            
        self.salvarArquivo("dashboard_especifica.json")
        self.conteudo = {}

    def dashboardProcessos(self):
        if self.df_processos.empty:
            return

        for mac in self.df_processos["mac_address"].unique():
            df_maquina = self.df_processos[self.df_processos["mac_address"] == mac]
            
            if mac not in self.conteudo:
                self.conteudo[mac] = {
                    "metricas": [],
                    "processos": []
                }
            
            processos_unicos = df_maquina["nome_processo"].unique()
            
            for nome in processos_unicos:
                df_historico_processo = df_maquina[df_maquina["nome_processo"] == nome]
                ultima_linha = df_historico_processo.iloc[-1]
                
                dadoProcesso = {
                    "tipoDado": "processo",
                    "macAddress": mac,
                    "nomeProcesso": nome,
                    "ultimaColeta": ultima_linha["data"],
                    "instancias": int(ultima_linha["instancias"]),
                    "cpuAtual": ultima_linha["cpu_total"],
                    "ramAtual": ultima_linha["ram_total"],
                    "grafico": {
                        "consumoRam": df_historico_processo["ram_total"].tolist(),
                        "consumoCpu": df_historico_processo["cpu_total"].tolist(),
                        "momento": df_historico_processo["data"].tolist()
                    }
                }
                
                if int(ultima_linha["cpu_total"]) > 1 or int(ultima_linha["ram_total"]) > 1:
                    self.conteudo[mac]["processos"].append(dadoProcesso)
                    
        self.salvarArquivo("dashboard_processos.json")
        self.conteudo = {}

    def mainLoop(self):
        self.dashboardAlertas()
        self.dashboardGestor()
        self.dashboardServidoresGerais()
        self.dashboardServidorEspecifico()
        self.dashboardProcessos()


def lambda_handler(event, context):
    try:
        client = Client(event)
        
        client.mainLoop()
        
        return {
            'statusCode': 200,
            'body': 'Arquivos json salvos'
        }
    except Exception as e:
        print(f"Erro: {str(e)}")
        raise e