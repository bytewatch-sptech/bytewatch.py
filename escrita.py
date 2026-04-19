import time, psutil, os, datetime, platform, uuid, boto3
from cpuinfo import get_cpu_info
from re import findall
import pandas as pd 

class Escrita():
    arquivoProcessos = ""
    arquivoMetricas = ""
    macAddress = ""
    cpu_nome = "Desconhecido"
    
    def __init__(self):
        self.macAddress = self.obterMacAddress()
        self.arquivoProcessos = f"processos_{self.macAddress}_raw.csv"
        self.arquivoMetricas = f"metricas_{self.macAddress}_raw.csv"
        self.obterDadoProcessador()

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

        return bytesRecebidos, bytesEnviados, velocidadeDownload, velocidadeUpload
    
    def capturarProcessos(self):
        num_cpus = psutil.cpu_count()
        total_ram = psutil.virtual_memory().total
        dadosProcesso = []
        horas = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        processos_info = list(psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent', 'memory_full_info']))
        quantidadeProcessos = len(processos_info)
        idProcessos = [proc.info.get('pid') for proc in processos_info]
        nomeProcesso = [proc.info.get('name') for proc in processos_info]
        usuarioProcesso = [proc.info.get('username') for proc in processos_info]
        consumoCPUProcesso = [(proc.info.get('cpu_percent', 0)) / num_cpus for proc in processos_info]
        
        consumoRAMProcesso = [((proc.info.get('memory_full_info').uss if proc.info.get('memory_full_info') else 0) / total_ram) * 100 for proc in processos_info]
        
        for i in range(len(idProcessos)):
            processos_dict = {"macAddress": self.macAddress, "quantidadeProcessos": quantidadeProcessos, "Data": horas, "idProcessos": idProcessos[i], "nomeProcesso": nomeProcesso[i], "usuarioProcesso": usuarioProcesso[i], "consumoCPUProcesso": consumoCPUProcesso[i], "consumoRAMProcesso": consumoRAMProcesso[i]}
            dadosProcesso.append(processos_dict)
        
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

        bytesRecebidos, bytesEnviados, velocidadeDownload, velocidadeUpload = self.pegarVelocidadeInternet()

        print(f"|    Escrita de métricas\n|    Horário: {horas}\n|    Endereço MAC: {self.macAddress}\n|    Nome da máquina: {nome_maquina}\n|\n======================================\n\n                CPU\n\nProcessador: {processador}\n\nCPU Porcentagem: {cpuPorcentagem}%\nCPU Núcleos Físicos: {cpuNucleosFisicos}\nCPU Núcleos Lógicos: {cpuNucleosLogicos}\n\nCPU Tempo Usuário: {cpuTempoUser}\nCPU Tempo Sistema: {cpuTempoSistema}\nCPU Tempo Inativo: {cpuTempoInativo}\n\n----------------------------------\n                RAM\n\nRAM Usada: {ramUsada}\nRAM Total: {ramTotal}\nRAM Livre: {ramLivre}\n\n----------------------------------\n                DISCO\n\nDisco Usado: {discoUsado}\nDisco Total: {discoTotal}\nDisco Livre: {discoLivre}\n\n----------------------------------\n                INTERNET\n\nTotal Bytes Enviados: {bytesEnviados}\nTotal Bytes Recebidos: {bytesRecebidos}\nVelocidade Download: {velocidadeDownload} B/s\nvelocidade Upload: {velocidadeUpload} B/s\n\n======================================")

        return {"horario": [horas], "macAddress": self.macAddress, "nome_maquina": [nome_maquina], "processador": [processador], "cpuPorcentagem": [cpuPorcentagem], "cpuNucleosFisicos": [cpuNucleosFisicos], "cpuNucleosLogicos": [cpuNucleosLogicos], "cpuTempoUser": [cpuTempoUser], "cpuTempoSistema": [cpuTempoSistema], "cpuTempoInativo": [cpuTempoInativo], "ramUsada": [ramUsada], "ramTotal": [ramTotal], "ramLivre": [ramLivre], "discoUsado": [discoUsado], "discoTotal": [discoTotal], "discoLivre": [discoLivre], "bytesEnviados": [bytesEnviados], "bytesRecebidos": [bytesRecebidos], "velocidadeDownload": [velocidadeDownload], "velocidadeUpload": [velocidadeUpload]}
