import time, psutil, os, datetime, platform, uuid, boto3, requests
from cpuinfo import get_cpu_info
from re import findall
import pandas as pd 

class Escrita():
    arquivoProcessos = ""
    arquivoMetricas = ""
    macAddress = ""
    cpu_nome = "Desconhecido"
    latitude = 0.0
    longitude = 0.0
    cidade = "None"
    
    def __init__(self):
        self.macAddress = self.obterMacAddress()
        self.arquivoProcessos = f"processos_{self.macAddress}_raw.csv"
        self.arquivoMetricas = f"metricas_{self.macAddress}_raw.csv"
        self.obterDadoProcessador()
        self.descobrirLocalizacao()

    def descobrirLocalizacao(self):
        try:
            resposta = requests.get("http://ip-api.com/json/").json()
            
            self.latitude = resposta.get("lat")
            self.longitude = resposta.get("lon")
            self.cidade = resposta.get("city")
            print(f"Servidor Localizado em: {self.cidade} ({self.latitude}, {self.longitude})")
        except Exception as e:
            print("Erro ao buscar localização")
            self.latitude = -23.5505
            self.longitude = -46.6333
            self.cidade = "São Paulo TM"    
    def obterMacAddress(self):
        mac = ':'.join(findall('..', '%012x' % uuid.getnode()))
        print(f"MAC Address: {mac}")
        return mac
    
    def obterDadoProcessador(self):
        info = get_cpu_info()
        self.cpu_nome = info['brand_raw']

    def salvarArquivo(self, dado, nomeArquivo):
        dataframe = pd.DataFrame(dado)
        if not os.path.exists(nomeArquivo):
            dataframe.to_csv(nomeArquivo, index=False)
        else:
            dataframe.to_csv(nomeArquivo, mode="a", header=False, index=False)

    def salvarArquivoNoBucket(self, file, bucket, path, fileName):
        s3 = boto3.client('s3')

        s3.upload_file(
            Filename=f'{file}',
            Bucket=f'{bucket}',    
            Key=f'{path}/{fileName}'
        )


    def pegarVelocidadeInternet(self):
        net_io = psutil.net_io_counters()
        bytesRecebidos = net_io.bytes_recv
        bytesEnviados = net_io.bytes_sent

        time.sleep(1)

        net_io_2 = psutil.net_io_counters()
        bytesRecebidos_2 = net_io_2.bytes_recv
        bytesEnviados_2 = net_io_2.bytes_sent

        velocidadeDownload = (bytesRecebidos_2 - bytesRecebidos)
        velocidadeUpload = (bytesEnviados_2 - bytesEnviados)

        droppedPackets = net_io.dropin + net_io.dropout

        conexoes_ativas = len(psutil.net_connections())

        return bytesRecebidos, bytesEnviados, velocidadeDownload, velocidadeUpload, droppedPackets, conexoes_ativas
    
    def capturarProcessos(self):
        num_cpus = psutil.cpu_count()
        total_ram = psutil.virtual_memory().total
        dadosProcesso = []
        horas = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        processos_info = list(psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_full_info', 'status']))
        quantidadeProcessos = len(processos_info)

        for proc in processos_info:
            try:
                info = proc.info
                mem_info = info.get('memory_full_info')
                
                status_capturado = info.get('status')
                status = "Ativo" if status_capturado in ['running', 'sleeping'] else "Inativo"
                
                uso_cpu = (info.get('cpu_percent', 0) or 0) / num_cpus
                uso_ram = ((mem_info.rss if mem_info else 0) / total_ram) * 100

                processos_dict = {
                    "macAddress": self.macAddress, 
                    "quantidadeProcessos": quantidadeProcessos, 
                    "Data": horas, 
                    "idProcessos": info.get('pid'), 
                    "nomeProcesso": info.get('name'), 
                    "usuarioProcesso": info.get('username'), 
                    "consumoCPUProcesso": round(uso_cpu, 2), 
                    "consumoRAMProcesso": round(uso_ram, 2),
                    "status": status 
                }
                dadosProcesso.append(processos_dict)
                
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        return dadosProcesso


    def obterInformacoesComponentes(self):
        nome_maquina = platform.node()
        processador = self.cpu_nome
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

        io_disco1 = psutil.disk_io_counters()
        time.sleep(1) 
        io_disco2 = psutil.disk_io_counters()
        velocidade_leitura = (io_disco2.read_bytes - io_disco1.read_bytes)
        velocidade_escrita = (io_disco2.write_bytes - io_disco1.write_bytes)

        bytesRecebidos, bytesEnviados, velocidadeDownload, velocidadeUpload, droppedPackets, conexoes_ativas = self.pegarVelocidadeInternet()

        print(f"|    Escrita de métricas\n|    Horário: {horas}\n|    Endereço MAC: {self.macAddress}\n|    Nome da máquina: {nome_maquina}\n|\n======================================\n\n                CPU\n\nProcessador: {processador}\n\nCPU Porcentagem: {cpuPorcentagem}%\nCPU Núcleos Físicos: {cpuNucleosFisicos}\nCPU Núcleos Lógicos: {cpuNucleosLogicos}\n\nCPU Tempo Usuário: {cpuTempoUser}\nCPU Tempo Sistema: {cpuTempoSistema}\nCPU Tempo Inativo: {cpuTempoInativo}\n\n----------------------------------\n                RAM\n\nRAM Usada: {ramUsada}\nRAM Total: {ramTotal}\nRAM Livre: {ramLivre}\n\n----------------------------------\n                DISCO\n\nDisco Usado: {discoUsado}\nDisco Total: {discoTotal}\nDisco Livre: {discoLivre}\n\n----------------------------------\n                INTERNET\n\nTotal Bytes Enviados: {bytesEnviados}\nTotal Bytes Recebidos: {bytesRecebidos}\nVelocidade Download: {velocidadeDownload} B/s\nvelocidade Upload: {velocidadeUpload} B/s\n\n======================================")

        return {"horario": [horas], "macAddress": self.macAddress, "nome_maquina": [nome_maquina], "latitude": [self.latitude], "longitude": [self.longitude], "cidade": [self.cidade], "processador": [processador], "cpuPorcentagem": [cpuPorcentagem], "cpuNucleosFisicos": [cpuNucleosFisicos], "cpuNucleosLogicos": [cpuNucleosLogicos], "cpuTempoUser": [cpuTempoUser], "cpuTempoSistema": [cpuTempoSistema], "cpuTempoInativo": [cpuTempoInativo], "ramUsada": [ramUsada], "ramTotal": [ramTotal], "ramLivre": [ramLivre], "discoUsado": [discoUsado], "discoTotal": [discoTotal], "discoLivre": [discoLivre], "velocidadeEscrita": [velocidade_escrita], "velocidadeLeitura": [velocidade_leitura], "bytesEnviados": [bytesEnviados], "bytesRecebidos": [bytesRecebidos], "velocidadeDownload": [velocidadeDownload], "velocidadeUpload": [velocidadeUpload], "droppedPackets": [droppedPackets], "conexoesAtivas": [conexoes_ativas]}
