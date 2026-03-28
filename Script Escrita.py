import psutil, datetime, time, os, json, platform
import speedtest
import pandas as pd 

def pegarVelocidadeDownload():
    try:
        s = speedtest.Speedtest()
        return s.download() / 1_000_000  # Mbps
    except Exception as e:
        print(f"Erro ao medir velocidade de download: {e}")
        return 0.0

def pegarVelocidadeUpload():
    try:
        s = speedtest.Speedtest()
        return s.upload() / 1_000_000  # Mbps
    except Exception as e:
        print(f"Erro ao medir velocidade de upload: {e}")
        return 0.0

data = datetime.date.today()

metricas = f"metricas_{data}.csv"
processos = f"processos_{data}.csv"



print(f'Inicializando processos de monitoramento...')

# loop infinito para definir e enviar as métricas para um arquivo CSV a cada 10 segundos.
while True:

    velocidadeDownload = pegarVelocidadeDownload()
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

    processos_info = list(psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent']))
    quantidadeProcessos = len(processos_info)
    idProcessos = [proc.info.get('pid') for proc in processos_info]
    nomeProcesso = [proc.info.get('name') for proc in processos_info]
    usuarioProcesso = [proc.info.get('username') for proc in processos_info]
    consumoCPUProcesso = [proc.info.get('cpu_percent', 0) for proc in processos_info]
    consumoRAMProcesso = [proc.info.get('memory_percent', 0) for proc in processos_info]



    # Imprime as métricas coletadas no terminal.
    print(f"""
                                |    Escrita de métricas
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
    

    print("\n" * 5)
    print("Coletando dados...")
    print("\n" * 5)

    # Definição dos dados a serem escritos no arquivo CSV.
    dados = {"horario": [horas], "nome_maquina": [nome_maquina], "cpuPorcentagem": [cpuPorcentagem], "cpuNucleosFisicos": [cpuNucleosFisicos], "cpuNucleosLogicos": [cpuNucleosLogicos], "cpuTempoUser": [cpuTempoUser], "cpuTempoSistema": [cpuTempoSistema], "cpuTempoInativo": [cpuTempoInativo], "ramUsada": [ramUsada], "ramTotal": [ramTotal], "ramLivre": [ramLivre], "discoUsado": [discoUsado], "discoTotal": [discoTotal], "discoLivre": [discoLivre], "velocidadeDownload": [velocidadeDownload], "velocidadeUpload": [velocidadeUpload]}

    #Criação do dataframe usando a biblioteca pandas.
    df_monitoramento = pd.DataFrame(dados)

    # Cria o arquivo CSV se ele não existir, ou anexa os dados se ele já existir.
    if not os.path.exists(metricas):
        while True:
            print("Criando arquivo de métricas... \n")
            try:
                df_monitoramento.to_csv(metricas, index=False)
                break
            except Exception as e:
                print(f"Erro ao criar {metricas}: {e}")
                time.sleep(1)
    else:
        df_monitoramento.to_csv(metricas, mode="a", header=False, index=False)

    for i in range(len(idProcessos)):

        processos_dict = {"quantidadeProcessos": [quantidadeProcessos], "Data": [horas], "idProcessos": [idProcessos[i]], "nomeProcesso": [nomeProcesso[i]], "usuarioProcesso": [usuarioProcesso[i]], "consumoCPUProcesso": [consumoCPUProcesso[i]], "consumoRAMProcesso": [consumoRAMProcesso[i]]}

        df_processos = pd.DataFrame(processos_dict)

        if not os.path.exists(processos):
            while True:
                print("Criando arquivo de processos... \n")
                try:
                    df_processos.to_csv(processos, index=False)
                    break
                except Exception as e:
                    print(f"Erro ao criar {processos}: {e}")
                    time.sleep(1)
        else:
            df_processos.to_csv(processos, mode="a", header=False, index=False)
    time.sleep(1800)
