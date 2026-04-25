import pandas as pd
import json

class Client:
    def __init__(self):
        self.conteudo = {}
        
        self.df_metrica = pd.read_csv("metricas_trusted.csv", on_bad_lines='skip')
        self.df_processos = pd.read_csv("processos_trusted.csv", on_bad_lines='skip')

    def tratarMetricas(self):
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
                "kpi": {
                    
                },
                "grafico": {
                    "megabytesEnviados": df_maquina["velocidadeUpload"].tolist(),
                    "megabytesRecebidos": df_maquina["velocidadeDownload"].tolist(),
                    "momento": df_maquina["horario"].tolist(),
                }
            })


    def tratarProcessos(self):
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
                if int(ultima_linha["cpu_total"]) > 1 or int(ultima_linha["ram_total"] > 1):
                    self.conteudo[mac]["processos"].append(dadoProcesso)

    def salvarArquivo(self):
        with open("client.json", 'w', encoding='utf-8') as f:
            json.dump(self.conteudo, f, indent=4, ensure_ascii=False)
    

client = Client()
client.tratarMetricas()
client.tratarProcessos()
client.salvarArquivo()