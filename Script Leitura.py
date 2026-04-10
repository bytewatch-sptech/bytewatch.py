import datetime, time, psutil, platform, sys, subprocess
import pandas as pd
import os
# Importação d|as bibliotecas necessárias para a leitura das métricas coletadas do sistema, assim como sua análise.

cpu_nome = "Desconhecido"

# Obtém o endereço MAC da interface de rede ativa (wlan0 ou enp6s0) para identificar a máquina.
mac_address = None
for interface, addrs in psutil.net_if_addrs().items():
    for addr in addrs:
        if addr.family == psutil.AF_LINK:
            if (interface == "wlan0" or interface == "enp6s0"):
                print(interface, addr.address)
                mac_address = addr.address

# Obtém o nome do processador dependendo do sistema operacional (Linux ou Windows).
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

# Horário de última leitura para evitar processar os mesmos dados repetidamente.
last_horario = None

# Função para carregar um arquivo CSV, retornando um DataFrame ou None em caso de erro.
def carregar_csv(caminho):
    try:
        return pd.read_csv(caminho)
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"Erro ao ler '{caminho}': {e}")
        return None

# Agrupa processos por nome, somando o consumo de CPU e RAM, junto do número de instâncias.
def agrupar_processos_por_nome(df_processos):
    # Verifica se o DataFrame é válido e contém as colunas necessárias.
    if not isinstance(df_processos, pd.DataFrame):
        return None, None
    if "Data" not in df_processos.columns or "nomeProcesso" not in df_processos.columns:
        return None, None

    # Obtém a data mais recente dos processos e filtra o DataFrame para essa data.
    ultima_data = df_processos["Data"].max()
    df_ultimo = df_processos[df_processos["Data"] == ultima_data].copy()
    if df_ultimo.empty:
        return ultima_data, None

    # Converte as colunas de consumo de CPU e RAM para numéricas e realiza a tratativa de erros.
    df_ultimo["consumoCPUProcesso"] = pd.to_numeric(df_ultimo["consumoCPUProcesso"], errors="coerce").fillna(0)
    df_ultimo["consumoRAMProcesso"] = pd.to_numeric(df_ultimo["consumoRAMProcesso"], errors="coerce").fillna(0)

    # Agrupa por nome do processo, contando instâncias e somando consumo de CPU e RAM.
    agrupado = df_ultimo.groupby("nomeProcesso", dropna=False, sort=False).agg(
        instancias=("nomeProcesso", "count"),
        cpu_total=("consumoCPUProcesso", "sum"),
        ram_total=("consumoRAMProcesso", "sum")
    ).reset_index()

    return ultima_data, agrupado.round({"cpu_total": 2, "ram_total": 2})


# loop infinito para ler o arquivo CSV a cada 30 minutos e processar os dados mais recentes.
while True:
    data = datetime.date.today()

    # Nomes dos arquivos CSV.
    metricas = f"metricas_{mac_address}_raw.csv"
    processos = f"processos_{mac_address}_raw.csv"
    resultados = "metricasTrusted.csv"
    processos_agregados = f"processosTrusted.csv"

    print("\nVerificando métricas e processos...")

    # Carrega os dados dos arquivos CSV, tratando erros de leitura.
    df = pd.read_csv(metricas, on_bad_lines='skip')
    df_processos = carregar_csv(processos)
    ultima_data_processos, processos_agrupados = agrupar_processos_por_nome(df_processos)

    horas = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # pega a última linha do DataFrame, que contém os dados mais recentes coletados.
    ultimo = df.iloc[-1]

    # Se arquivo não tiver sido atualizado desde a última leitura, espera 10 segundos e continua o loop.
    if ultimo["horario"] == last_horario:
        print(f"sem novos dados desde {last_horario}, aguardando próxima leitura...")
        time.sleep(10)
        continue

    last_horario = ultimo["horario"]


    nome_maquina = platform.node();
    processador = cpu_nome

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

    mediaRam   = round(df["ramUsada"].mean() / 1024**3, 2)
    porcentagemRam = round((ramUsada / ramTotal) * 100, 2)
    mediaDisco = round(df["discoUsado"].mean() / 1024**3, 2)
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

    # Cria o arquivo CSV se ele não existir, ou anexa os dados se ele já existir.
    df_resultados = pd.DataFrame(dados_resultados)
    if not os.path.exists(resultados):
        df_resultados.to_csv(resultados, index=False)
    else:
        df_resultados.to_csv(resultados, mode="a", header=False, index=False)

    if processos_agrupados is not None:
        processos_agrupados.to_csv(processos_agregados, index=False)

    # Imprime os dados mais recentes e as médias no terminal.
    print(f"""
                                |    Leitura de métricas
                                |    Horário: {horas}
                                |    Nome da máquina: {nome_maquina}
                                |    Endereço MAC: {macAddress}
                                |          
                                ======================================

                                                CPU
                                            
                                Processador: {processador}
                                
                                CPU Porcentagem: {cpuPorcentagem}%
                                CPU Núcleos Físicos: {cpuNucleosFisicos}
                                CPU Núcleos Lógicos: {cpuNucleosLogicos}

                                CPU Tempo Usuário: {cpuTempoUser} minutos
                                CPU Tempo Sistema: {cpuTempoSistema} minutos
                                CPU Tempo Inativo: {cpuTempoInativo} minutos

                                ----------------------------------
                                                RAM

                                RAM Usada: {ramUsada} GB
                                RAM Total: {ramTotal} GB
                                RAM Livre: {ramLivre} GB

                                ----------------------------------
                                                DISCO

                                Disco Usado: {discoUsado} GB
                                Disco Total: {discoTotal} GB
                                Disco Livre: {discoLivre} GB

                                ----------------------------------
                                               MÉDIAS

                                Porcentagem RAM Usada: {porcentagemRam}%
                                Média RAM Usada: {mediaRam} GB

                                Porcentagem Disco Usado: {porcentagemDisco}%
                                Média Disco Usado: {mediaDisco} GB

                                ----------------------------------
                                                INTERNET

                                Total Megabytes Enviados: {megabytesEnviados} MB
                                Total Megabytes Recebidos: {megabytesRecebidos} MB
                                Velocidade Download: {velocidadeDownload} Mbps
                                Velocidade Upload: {velocidadeUpload} Mbps

                                ======================================
""")
    print("\nAguardando próxima leitura...\n")

    time.sleep(1)# Aguarda 30 minutos antes de realizar a próxima leitura.