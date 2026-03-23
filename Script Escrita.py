import psutil, datetime, time, os, json, platform
import speedtest
import pandas as pd 


def pegarVelocidadeInternet():
    s = speedtest.Speedtest()
    return s.download() / 1_000_000  # Mbps

def pegarVelocidadeUpload():
    s = speedtest.Speedtest()
    return s.upload() / 1_000_000  # Mbps

if os.name == 'nt':
    #Diferentes definições de métricas baseada por compatilibildade do sistema operacional.

    

    metricas = "metricasPandasWin.csv"
    processos = "processosPandasWin.csv"

    print(f'{metricas} e {processos} criados com sucesso!')

    # loop infinito para definir e enviar as métricas para um arquivo CSV a cada 10 segundos.
    while True:
        velocidadeDownload = pegarVelocidadeInternet()
        velocidadeUpload = pegarVelocidadeUpload()
        nome_maquina = platform.node()
        horas = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cpuPorcentagem = psutil.cpu_percent()
        cpuNucleosFisicos = psutil.cpu_count(logical=False)
        cpuNucleosLogicos = psutil.cpu_count()

        cpuTempoUser = psutil.cpu_times().user
        cpuTempoSistema = psutil.cpu_times().system
        cpuTempoInativo = psutil.cpu_times().idle

        ramLivre = (psutil.virtual_memory().available)
        ramUsada = (psutil.virtual_memory().used)
        ramTotal = (psutil.virtual_memory().total)

        discoLivre = (psutil.disk_usage("C:\\").free)
        discoUsado = (psutil.disk_usage("C:\\").used)
        discoTotal = (psutil.disk_usage("C:\\").total)

        quantidadeProcessos = len(psutil.pids())
        informacaoProcessos = [f"PID: {proc.info['pid']}, Nome: {proc.info['name']}, Usuário: {proc.info['username']}, CPU: {proc.info.get('cpu_percent', 0)}, RAM: {proc.info.get('memory_percent', 0)}%" 
                            for proc in psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent'])]
        
        informacaoProcessosJson = json.dumps(informacaoProcessos)  # Converte para JSON

        #Imprime as métricas coletadas no terminal.
        print(f"""
                                    |    Escrita de métricas - Windows
                                    |    Horário: {horas}
                                    |    Nome da máquina: {nome_maquina}
                                    |          
                                    ======================================

                                                    CPU
                                    
                                    CPU Porcentagem: {cpuPorcentagem}%
                                    CPU Núcleos Físicos: {cpuNucleosFisicos}
                                    CPU Núcleos Lógicos: {cpuNucleosLogicos}

                                    CPU Tempo Usuário: {cpuTempoUser}
                                    CPU Tempo Sistema: {cpuTempoSistema}
                                    CPU Tempo Inativo: {cpuTempoInativo}

                                    ----------------------------------
                                                    RAM

                                    RAM Usada: {ramUsada}
                                    RAM Total: {ramTotal} 
                                    RAM Livre: {ramLivre}

                                    ----------------------------------
                                                   DISCO
                                    Disco Usado: {discoUsado}
                                    Disco Total: {discoTotal} 
                                    Disco Livre: {discoLivre}

                                    ----------------------------------
                                                 INTERNET

                                    Velocidade Download: {velocidadeDownload} Mbps
                                    Velocidade Upload: {velocidadeUpload} Mbps

                                    ======================================
    """)
        

        # Definição dos dados a serem escritos no arquivo CSV.
        dados = {"horario": [horas], "cpuPorcentagem": [cpuPorcentagem], "cpuNucleosFisicos": [cpuNucleosFisicos], "cpuNucleosLogicos": [cpuNucleosLogicos], "cpuTempoUser": [cpuTempoUser], "cpuTempoSistema": [cpuTempoSistema], "cpuTempoInativo": [cpuTempoInativo], "ramUsada": [ramUsada], "ramTotal": [ramTotal], "ramLivre": [ramLivre], "discoUsado": [discoUsado], "discoTotal": [discoTotal], "discoLivre": [discoLivre], "velocidadeDownload": [velocidadeDownload], "velocidadeUpload": [velocidadeUpload]}

        #Criação do dataframe usando a biblioteca pandas.
        df_monitoramento = pd.DataFrame(dados)

        # Cria o arquivo CSV se ele não existir, ou anexa os dados se ele já existir.
        if not os.path.exists(metricas):
            df_monitoramento.to_csv(metricas, index=False)
        else:
            df_monitoramento.to_csv(metricas, mode="a", header=False, index=False)
        time.sleep(10)

        processos_dict = {"quantidadeProcessos": [quantidadeProcessos], "informacaoProcessos": [informacaoProcessosJson]}

        df_processos = pd.DataFrame(processos_dict)

        if not os.path.exists(processos):
            df_processos.to_csv(processos, index=False)
        else:
            df_processos.to_csv(processos, mode="a", header=False, index=False)
        time.sleep(10)

else:

    metricas = "metricasPandaslnx.csv"
    processos = "processosPandaslnx.csv"

    print(f'{metricas} criado com sucesso!')

    # loop infinito para definir e enviar as métricas para um arquivo CSV a cada 10 segundos.
    while True:
        velocidadeDownload = pegarVelocidadeInternet()
        velocidadeUpload = pegarVelocidadeUpload()
        nome_maquina = platform.node()
        horas = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        cpuPorcentagem = psutil.cpu_percent()
        cpuNucleosFisicos = psutil.cpu_count(logical=False)
        cpuNucleosLogicos = psutil.cpu_count()

        cpuTempoUser = psutil.cpu_times().user
        cpuTempoSistema = psutil.cpu_times().system
        cpuTempoInativo = psutil.cpu_times().idle

        temperaturaCapturada = psutil.sensors_temperatures()
        ventoinhas = psutil.sensors_fans()

        ramLivre = (psutil.virtual_memory().available)
        ramUsada = (psutil.virtual_memory().used)
        ramTotal = (psutil.virtual_memory().total)

        discoLivre = (psutil.disk_usage("C:\\").free)
        discoUsado = (psutil.disk_usage("C:\\").used)
        discoTotal = (psutil.disk_usage("C:\\").total)

        quantidadeProcessos = len(psutil.pids())
        informacaoProcessos = [f"PID: {proc.info['pid']}, Nome: {proc.info['name']}, Usuário: {proc.info['username']}, CPU: {proc.info.get('cpu_percent', 0)}, RAM: {proc.info.get('memory_percent', 0)}%" 
                            for proc in psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent'])]
        
        informacaoProcessosJson = json.dumps(informacaoProcessos)  # Converte para JSON

        #Imprime as métricas coletadas no terminal.
        print(f"""
                                    |    Escrita de métricas - Linux
                                    |    Horário: {horas}
                                    |    Nome da máquina: {nome_maquina}
                                    |          
                                    ======================================

                                                    CPU
                                    
                                    CPU Porcentagem: {cpuPorcentagem}%
                                    CPU Núcleos Físicos: {cpuNucleosFisicos}
                                    CPU Núcleos Lógicos: {cpuNucleosLogicos}

                                    CPU Tempo Usuário: {cpuTempoUser}
                                    CPU Tempo Sistema: {cpuTempoSistema}
                                    CPU Tempo Inativo: {cpuTempoInativo}

                                    ----------------------------------
                                                    RAM

                                    RAM Usada: {ramUsada}
                                    RAM Total: {ramTotal} 
                                    RAM Livre: {ramLivre}

                                    ----------------------------------
                                                   DISCO
                                    Disco Usado: {discoUsado}
                                    Disco Total: {discoTotal} 
                                    Disco Livre: {discoLivre}

                                    ----------------------------------
                                                 INTERNET

                                    Velocidade Download: {velocidadeDownload} Mbps
                                    Velocidade Upload: {velocidadeUpload} Mbps

                                    ----------------------------------
                                                 TEMPERATURA 

                                    Temperatura CPU: {temperaturaCapturada.get('coretemp', [{'current': 'N/A'}])[0]['current']} °C
                                    Temperatura GPU: {temperaturaCapturada.get('amdgpu', [{'current': 'N/A'}])[0]['current']} °C
                                    Velocidade Ventoinha CPU: {ventoinhas.get('cpu_fan', [{'current': 'N/A'}])[0]['current']} RPM
                                    Velocidade Ventoinha GPU: {ventoinhas.get('gpu_fan', [{'current': 'N/A'}])[0]['current']} RPM 
                                    

                                    ======================================
    """)
        
        if "coretemp" in temps:  # CPUs Intel
                cpu_temp = max([t.current for t in temps["coretemp"]])
        elif "k10temp" in temps:  # AMD
                cpu_temp = max([t.current for t in temps["k10temp"]])
        
        if "amdgpu" in temps:
            gpu_temp = max([t.current for t in temps["amdgpu"]])
        elif "nvidia" in temps:
            gpu_temp = max([t.current for t in temps["nvidia"]])

        fans = psutil.sensors_fans()
        cpu_fan = 0
        gpu_fan = 0
        cpu_temp = 0
        gpu_temp = 0

        for nome_sensor, entradas in fans.items():
            for fan in entradas:
                label = fan.label.lower()
                if "cpu" in label:
                    cpu_fan = fan.current
                elif "gpu" in label:
                    gpu_fan = fan.current
        
        # Definição dos dados a serem escritos no arquivo CSV.
        dados = {"horario": [horas], "cpuPorcentagem": [cpuPorcentagem], "cpuNucleosFisicos": [cpuNucleosFisicos], "cpuNucleosLogicos": [cpuNucleosLogicos], "cpuTempoUser": [cpuTempoUser], "cpuTempoSistema": [cpuTempoSistema], "cpuTempoInativo": [cpuTempoInativo], "ramUsada": [ramUsada], "ramTotal": [ramTotal], "ramLivre": [ramLivre], "discoUsado": [discoUsado], "discoTotal": [discoTotal], "discoLivre": [discoLivre], "cpu_temp": [cpu_temp], "gpu_temp": [gpu_temp], "cpu_fan": [cpu_fan], "gpu_fan": [gpu_fan], "velocidadeDownload": [velocidadeDownload], "velocidadeUpload": [velocidadeUpload]}

        #Criação do dataframe usando a biblioteca pandas.
        df_monitoramento = pd.DataFrame(dados)

        # Cria o arquivo CSV se ele não existir, ou anexa os dados se ele já existir.
        if not os.path.exists(metricas):
            df_monitoramento.to_csv(metricas, index=False)
        else:
            df_monitoramento.to_csv(metricas, mode="a", header=False, index=False)
        time.sleep(10)

        processos_dict = {"quantidadeProcessos": [quantidadeProcessos], "informacaoProcessos": [informacaoProcessosJson]}

        df_processos = pd.DataFrame(processos_dict)

        if not os.path.exists(processos):
            df_processos.to_csv(processos, index=False)
        else:
            df_processos.to_csv(processos, mode="a", header=False, index=False)
        time.sleep(10)