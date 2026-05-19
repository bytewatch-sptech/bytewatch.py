import pandas as pd
import json

class Client:
    def __init__(self):
        self.conteudo = {}
        
        self.df_metrica = pd.read_csv("metricas_trusted.csv", on_bad_lines='skip')
        self.df_processos = pd.read_csv("processos_trusted.csv", on_bad_lines='skip')

    def classificar_alerta(self, valor):
        if valor >= 90:
            print("critico")
            return "CRÍTICO"
        elif valor >= 70:
            print("alerta")
            return "ALERTA"
        return None

    def adicionar_alerta(self, mac, componente, valor, horario):
        nivel = self.classificar_alerta(valor)

        if nivel:
            self.conteudo["alertas"].append(
                {
                    "mac": mac,
                    "componente": componente.upper(),
                    "nivel": nivel,
                    "valor": float(valor),
                    "horario": horario,
                    "mensagem": f"Uso de {componente.upper()} em {valor}%",
                }
            )

    def prioridade_alerta(self, alerta):
        if alerta["nivel"] == "CRÍTICO":
            return 0
        else:
            return 1

    def dashboardAlertasGestor(self):
        for mac in self.df_metrica["macAddress"].unique():
            df_maquina = self.df_metrica[self.df_metrica["macAddress"] == mac]
            ultima_linha = df_maquina
            # ultima_linha = df_maquina.iloc[-1]

            if mac not in self.conteudo:
                self.conteudo = {"alertas": []}

                print((ultima_linha.porcentagemRam))

                ultima_linha.porcentagemRam = list(ultima_linha.porcentagemRam)
                ultima_linha.horario = list(ultima_linha.horario)

                for i in range(len(ultima_linha.porcentagemRam)): 
                    self.adicionar_alerta(
                        mac, "ram", ultima_linha.porcentagemRam[i], ultima_linha.horario[i]
                    )
                    


                ultima_linha.porcentagemDisco = list(ultima_linha.porcentagemDisco)

                for i in range(len(ultima_linha.porcentagemDisco)): 
                    self.adicionar_alerta(
                        mac, "disco", ultima_linha.porcentagemDisco[i], ultima_linha.horario[i]
                    )

                ultima_linha.cpuPorcentagem = list(ultima_linha.cpuPorcentagem)

                for i in range(len(ultima_linha.cpuPorcentagem)): 
                    self.adicionar_alerta(
                        mac, "cpu", ultima_linha.cpuPorcentagem[i], ultima_linha.horario[i]
                    )

                self.conteudo["alertas"] = sorted(
                    self.conteudo["alertas"], key=self.prioridade_alerta
                )

        self.salvarArquivo("dashboard_alertas.json")
        self.conteudo = {}

    def dashboardGestor(self):
        df_maquina = self.df_metrica.copy()
        df_maquina['horario'] = pd.to_datetime(df_maquina['horario'])
        
        df_maquina = df_maquina.sort_values(by='horario')
        df_ultimas_coletas = df_maquina.groupby('macAddress').last().reset_index()

        data_minima = df_maquina['horario'].min()
        data_maxima = df_maquina['horario'].max()
        dias_passados = (data_maxima - data_minima).days

        total_ram = df_ultimas_coletas['ramTotal'].sum()
        total_disco = df_ultimas_coletas['discoTotal'].sum()

        custoDiarioRam = (total_ram * 2.50) / 2
        custoDiarioDisco = (total_disco * 0.0133) / 1

        custoMensalRam = custoDiarioRam * 30
        custoMensalDisco = custoDiarioDisco * 30
        custoTotalMensal = custoMensalRam + custoMensalDisco

        custoAteAgoraDisco = custoDiarioDisco * dias_passados
        custoAteAgoraRam = custoDiarioRam * dias_passados
        custoTotalAteAgora = custoAteAgoraDisco + custoAteAgoraRam

        
        self.conteudo = {
            "primeiraColeta": data_minima.strftime('%Y-%m-%d %H:%M:%S'),
            "ultimaColeta": data_maxima.strftime('%Y-%m-%d %H:%M:%S'),
            "diasDecorridos": int(dias_passados),
            "ramTotal": float(total_ram),
            "discoTotal": float(total_disco), 
            "custoTotalAteAgora": round(custoTotalAteAgora, 2),
            "custoTotalNoMes": round(custoTotalMensal, 2)
        }
           
        self.salvarArquivo("dashboard_gestor.json")
        self.conteudo = {}


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

            cpu_val = float(ultima_linha.cpuPorcentagem)
            ram_val = float(ultima_linha.porcentagemRam)
            disco_val = float(ultima_linha.porcentagemDisco)

            vel_upload = float(ultima_linha.velocidadeUpload)
            vel_download = float(ultima_linha.velocidadeDownload)
            dropped = int(ultima_linha.droppedPackets)

            lat_real = float(ultima_linha.get("latitude", -23.5505))
            lon_real = float(ultima_linha.get("longitude", -46.6333))
            cidade_real = str(ultima_linha.get("cidade", "São Paulo"))
            capacidade_nominal_MB = 125.0

            saturacao_rede = ((vel_upload + vel_download)/ capacidade_nominal_MB) * 100
            p_saturacao = (saturacao_rede - 70) * 1.5 if saturacao_rede > 70 else 0.0
            p_erro = dropped * 2.0
            rede_eficiencia = max(0.0, min(100.0, 100.0 - p_saturacao - p_erro))

            rede_eficiencia_invertida = 100.0 - rede_eficiencia
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

            eixo_x = (cpu_val * 0.50) + (ram_val * 0.30) + (disco_val * 0.10) + (rede_eficiencia_invertida * 0.10)
            eixo_y = min(100.0, float(dropped) * 1.0)

            tamanho_bolha = max(eixo_x, eixo_y)


            servidor_objeto = {
                "macAddress": mac,
                "nome": nome_maquina,
                "regiao": cidade_real,
                "ultimaColeta": str(ultima_linha.horario),
                "status_servidor": status_servidor,
                "diagnostico_etl": texto_diagnostico,
                "geolocalizacao": {
                    "coords": [lon_real, lat_real],
                    "saturacao_trafego": round(saturacao_rede, 1)
                },
                "matriz_priorizacao":{
                    "x": round(eixo_x, 1),
                    "y": round(eixo_y, 1),
                    "tamanho_bolha": round(tamanho_bolha, 1)
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
        self.conteudo = {}

    def calcular_previsao_esgotamento(self, df_maquina):
        data_ultima_captura = df_maquina['horario'].max().normalize()
        df_maquina = df_maquina[df_maquina['horario'] >= data_ultima_captura].copy()
        n_pontos = len(df_maquina)

        if n_pontos < 2:
            return "Sem previsão"
        
        minutos_passados = (df_maquina['horario'] - data_ultima_captura).dt.total_seconds() / 60.0
        eixo_y = df_maquina['porcentagemRam']
        
        soma_x = minutos_passados.sum() 
        soma_y = eixo_y.sum()
        soma_xy = (minutos_passados * eixo_y).sum()
        soma_x2 = (minutos_passados ** 2).sum()
        
        denominador = (n_pontos * soma_x2) - (soma_x ** 2)
        
        if denominador == 0:
            return "Sem previsão"
        
        # a = (n * Σ(xy) - Σx * Σy) / (n * Σ(x²) - (Σx)²)
        a = ((n_pontos * soma_xy) - (soma_x * soma_y)) / denominador

        # b = (Σy - a * Σx) / n                    
        b = (soma_y - (a * soma_x)) / n_pontos

        print(f"coeficiente argular: {a} \ncoeficiente linear: {b}")
        
        if a > 0:
            # x = tempo
            # y = consumoRam
            # y = ax + b
            # 100 = ax + b
            # 100 - b = a * x
            # x = (100 - b) / a
            minuto_esgotamento = (100 - b) / a
            ultimo_minuto = minutos_passados.iloc[-1]
            minutos_restantes = minuto_esgotamento - ultimo_minuto
            
            if minutos_restantes <= 0:
                return "Limite atingido"
            else:
                horas = int(minutos_restantes // 60)
                minutos = int(minutos_restantes % 60)
                return f"{horas}h {minutos}min"
                
        return "Sem previsão"

    def dashboardRam(self):
        for mac in self.df_metrica["macAddress"].unique():
            df_maquina = self.df_metrica[self.df_metrica["macAddress"] == mac]
            df_processos = self.df_processos[self.df_processos["mac_address"] == mac]
            df_maquina['horario'] = pd.to_datetime(df_maquina['horario'])
            df_maquina = df_maquina.sort_values(by='horario')

            data_ultima_captura = df_maquina['horario'].max().normalize() 
            data_ontem = data_ultima_captura - pd.Timedelta(days=1)       

            df_hoje = df_maquina[df_maquina['horario'] >= data_ultima_captura]
            df_ontem = df_maquina[(df_maquina['horario'] >= data_ontem) & (df_maquina['horario'] < data_ultima_captura)]

            pico_hoje = 0
            pico_ontem = 0
            if (len(df_hoje) > 0):
                pico_hoje = int(round(df_hoje['porcentagemRam'].max(), 0))
            if (len(df_ontem) > 0):
                pico_ontem = int(round(df_ontem['porcentagemRam'].max(), 0))

            variacao = pico_hoje - pico_ontem

            ultima_linha = df_maquina.iloc[-1]

            tempo_esgotamento = self.calcular_previsao_esgotamento(df_maquina)
            
            if mac not in self.conteudo:
                self.conteudo[mac] = {
                    "metricas": [],
                    "processos": []
                }
            self.conteudo[mac]["metricas"] = {
                "tipoDado": "ram",
                "macAddress": mac,
                "ultimaColeta": ultima_linha.horario.strftime('%Y-%m-%d %H:%M:%S'),
                "porcentagemRam": ultima_linha.porcentagemRam,
                "ramTotal": ultima_linha.ramTotal,
                "ramUsada": ultima_linha.ramUsada,
                "kpiUso": {
                    "percentualUsado": round(ultima_linha.porcentagemRam),
                    "percentualLivre": round(100 - ultima_linha.porcentagemRam)
                },
                "kpiPico": {
                    "picoHoje": pico_hoje,
                    "picoOntem": pico_ontem,
                    "variacao": variacao
                },
                "kpiInformacao": {
                    "macAddress": ultima_linha.macAddress,
                    "ultimaColeta": ultima_linha.horario.strftime('%Y-%m-%d %H:%M:%S'),
                    "capacidadeRam": round(ultima_linha.ramTotal),
                },
                "kpiEsgotamento": {
                    "tempoRestante": tempo_esgotamento
                },
                "grafico": {
                    "percentualUsado": df_maquina["porcentagemRam"].tail(7).tolist(),
                    "momento": df_maquina["horario"].tail(7).dt.strftime('%H:%M').tolist(),
                }
            }

            processos_unicos = df_processos["nome_processo"].unique()
            
            for nome in processos_unicos:
                df_historico_processo = df_processos[df_processos["nome_processo"] == nome]
                ultima_linha = df_historico_processo.iloc[-1]
                
                dadoProcesso = {
                    "tipoDado": "processo",
                    "quantidadeProcessos": int(ultima_linha["quantidadeProcessos"]),
                    "pid": int(ultima_linha["pid"]),
                    "status": ultima_linha["status"],
                    "macAddress": mac,
                    "nomeProcesso": nome,
                    "ultimaColeta": ultima_linha["data"],
                    "instancias": int(ultima_linha["instancias"]),
                    "percentualRam": round(ultima_linha["ram_total"], 2),
                }
                if int(ultima_linha["ram_total"]) > 0.5:
                    self.conteudo[mac]["processos"].append(dadoProcesso)

        self.salvarArquivo("dashboard_ram.json")

    def salvarArquivo(self, nomeArquivo):
        with open(nomeArquivo, 'w', encoding='utf-8') as f:
            json.dump(self.conteudo, f, indent=4, ensure_ascii=False)

    def mainLoop(self):
        self.dashboardAlertasGestor()
        self.dashboardGestor()
        self.dashboardServidoresGerais()
        self.dashboardRam()
    

