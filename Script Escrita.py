import psutil, datetime, time, os, json, platform, subprocess, sys
import pandas as pd 
import boto3

mac_address = None
for interface, addrs in psutil.net_if_addrs().items():
    for addr in addrs:
        if addr.family == psutil.AF_LINK:
            if (interface == "wlan0" or interface == "enp6s0"):
                print(interface, addr.address)
                mac_address = addr.address

# bucket = ""

# def salvar_arquivo(file, bucket, path, fileName):
#     s3 = boto3.client('s3')

#     s3.upload_file(
#         Filename=f'{file}',
#         Bucket=f'{bucket}',    
#         Key=f'{path}/{fileName}'
#     )

cpu_nome = "Desconhecido"

try:
    if platform.system() == "Linux":
        comando = subprocess.run(
            "cat /proc/cpuinfo | grep 'model name' | head -n 1",
            shell=True,
            capture_output=True,
            text=True
        )

        cpu_nome = comando.stdout.split(":")[1].strip()

    elif platform.system() == "Windows":
        comando = subprocess.run(
            "wmic cpu get name",
            shell=True,
            capture_output=True,
            text=True
        )

        cpu_nome = platform.processor() 
        # comando.stdout.split("=")[1].strip()

    else:
        print("Erro: Plataforma não suportada!")    
        sys.exit(1)
except Exception as excecao:
    print(f"Falha ao obter informaçõesda CPU: {excecao}")

data = datetime.date.today()


metricas = f"metricas_{mac_address}_raw.csv"
processos = f"processos_{mac_address}_raw.csv"

def pegarVelocidadeInternet():
    net_io = psutil.net_io_counters()
    bytesRecebidos = net_io.bytes_recv
    bytesEnviados = net_io.bytes_sent

    time.sleep(1)

    net_io_2 = psutil.net_io_counters()
    bytesRecebidos_2 = net_io_2.bytes_recv
    bytesEnviados_2 = net_io_2.bytes_sent

    velocidadeDownload = (bytesRecebidos_2 - bytesRecebidos)
    velocidadeUpload = (bytesEnviados_2 - bytesEnviados)

    return bytesRecebidos, bytesEnviados, velocidadeDownload, velocidadeUpload


print(f'Inicializando processos de monitoramento...')
def capturarProcessos():
    processos_info = list(psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent']))
    quantidadeProcessos = len(processos_info)
    idProcessos = [proc.info.get('pid') for proc in processos_info]
    nomeProcesso = [proc.info.get('name') for proc in processos_info]
    usuarioProcesso = [proc.info.get('username') for proc in processos_info]
    consumoCPUProcesso = [proc.info.get('cpu_percent', 0) for proc in processos_info]
    consumoRAMProcesso = [proc.info.get('memory_percent', 0) for proc in processos_info]

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

# loop infinito para definir e enviar as métricas para um arquivo CSV a cada 10 segundos.
while True:

    bytesRecebidos, bytesEnviados, velocidadeDownload, velocidadeUpload = pegarVelocidadeInternet()

    nome_maquina = platform.node()
    processador = cpu_nome
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

    discoLivre = (psutil.disk_usage("/").free)
    discoUsado = (psutil.disk_usage("/").used)
    discoTotal = (psutil.disk_usage("/").total)



    # Imprime as métricas coletadas no terminal.
    print(f"""
                                |    Escrita de métricas
                                |    Horário: {horas}
                                |    Endereço MAC: {mac_address}
                                |    Nome da máquina: {nome_maquina}
                                |          
                                ======================================

                                                CPU

                                Processador: {processador}
                                
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

                                Total Bytes Enviados: {bytesEnviados}
                                Total Bytes Recebidos: {bytesRecebidos} 
                                Velocidade Download: {velocidadeDownload} B/s
                                velocidade Upload: {velocidadeUpload} B/s

                                ======================================
""")
    

    print("\n" * 5)
    print("Coletando dados...")
    print("\n" * 5)

    # Definição dos dados a serem escritos no arquivo CSV.
    dados = {"horario": [horas], "macAddress": mac_address, "nome_maquina": [nome_maquina], "processador": [processador], "cpuPorcentagem": [cpuPorcentagem], "cpuNucleosFisicos": [cpuNucleosFisicos], "cpuNucleosLogicos": [cpuNucleosLogicos], "cpuTempoUser": [cpuTempoUser], "cpuTempoSistema": [cpuTempoSistema], "cpuTempoInativo": [cpuTempoInativo], "ramUsada": [ramUsada], "ramTotal": [ramTotal], "ramLivre": [ramLivre], "discoUsado": [discoUsado], "discoTotal": [discoTotal], "discoLivre": [discoLivre], "bytesEnviados": [bytesEnviados], "bytesRecebidos": [bytesRecebidos], "velocidadeDownload": [velocidadeDownload], "velocidadeUpload": [velocidadeUpload]}

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

    capturarProcessos()
    
    # salvar_arquivo(metricas, bucket, "maquina1/bruto", metricas)

    time.sleep(1)
