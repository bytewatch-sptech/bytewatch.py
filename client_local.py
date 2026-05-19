import pandas as pd
import json

class Client:
    def __init__(self):
        self.conteudo = {}
        
        self.df_metrica = pd.read_csv("metricas_trusted.csv", on_bad_lines='skip')
        self.df_processos = pd.read_csv("processos_trusted.csv", on_bad_lines='skip')

    def dashboardAlertas(self):
        self.salvarArquivo("dashboard_alertas.json")

    def dashboardGestor(self):
        if self.df_metrica.empty:
            return

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
        
        for mac in self.df_metrica["macAddress"].unique():
            df_maquina = self.df_metrica[self.df_metrica["macAddress"] == mac]
            ultima_captura = df_maquina.iloc[-1]

            idx_ram = df_maquina["porcentagemRam"].idxmax()
            pico_ram = df_maquina.loc[idx_ram, "porcentagemRam"]
            momento_pico_ram = df_maquina.loc[idx_ram, "horario"]

            idx_cpu = df_maquina["cpuPorcentagem"].idxmax()
            pico_cpu = df_maquina.loc[idx_cpu, "cpuPorcentagem"]
            momento_pico_cpu = df_maquina.loc[idx_cpu, "horario"]

            if mac not in self.conteudo:
                self.conteudo[mac] = {
                    "metricas": [],
                    "processos": []
                }

            self.conteudo[mac]["metricas"].append({
                "tipoDado": "ram",
                "macAddress": mac,
                "ultimaColeta": ultima_captura.horario,
                "porcentagemRam": ultima_captura.porcentagemRam,
                "ramTotal": ultima_captura.ramTotal,
                "ramUsada": ultima_captura.ramUsada,
                "kpi": {
                    "percentualUsado": ultima_captura.porcentagemRam,
                    "percentualLivre": 100 - ultima_captura.porcentagemRam
                },
                "grafico": {
                    "percentualUsado": ultima_captura.porcentagemRam,
                    "percentualLivre": 100 - ultima_captura.porcentagemRam,
                    "pico": pico_ram,
                    "momentoPico": momento_pico_ram
                }
            })

            self.conteudo[mac]["metricas"].append({
                "tipoDado": "cpu",
                "macAddress": mac,
                "ultimaColeta": ultima_captura.horario,
                "processador": ultima_captura.processador,
                "porcentagemCpu": ultima_captura.cpuPorcentagem,
                "coresLogicos": int(ultima_captura.cpuNucleosLogicos),
                "kpi": {
                    "percentualUsado": ultima_captura.cpuPorcentagem,
                    "percentualLivre": 100 - ultima_captura.cpuPorcentagem
                },
                "grafico": {
                    "percentualUsado": ultima_captura.cpuPorcentagem,
                    "percentualLivre": 100 - ultima_captura.cpuPorcentagem,
                    "pico": pico_cpu,
                    "momentoPico": momento_pico_cpu
                }
            })

            

        self.salvarArquivo("dashboard_gestor.json")
        self.conteudo = {}

        
           
        self.salvarArquivo("dashboard_gestor.json")
        self.conteudo = {}


    def dashboardServidoresGerais(self):
        self.salvarArquivo("dashboard_geral.json")

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
        self.dashboardAlertas()
        self.dashboardGestor()
        self.dashboardServidoresGerais()
        self.dashboardRam()
    

