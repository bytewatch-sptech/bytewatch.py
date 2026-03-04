import psutil
import csv
import platform
import time
import speedtest
from datetime import datetime

filename = "dados_maquina.csv"
headers = ["Data/Hora", "Maquina", "CPU_Uso_%", "RAM_Total_GB", "RAM_Uso_%", "Disco_%" "Download_mbps", "Upload_mbps"]

def coletar_dados():
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nome_maquina = platform.node()
    cpu_percent = psutil.cpu_percent(interval=1)
    memoria = psutil.virtual_memory()
    ram_total = round(memoria.total / (1024**3), 2) 
    ram_percent = memoria.percent
    disk = psutil.disk_usage('/')
    st = speedtest.Speedtest()
    download = st.download() / 10**6  
    download2 = round(download, 2)
    upload = st.upload() / 10**6
    upload2 = round(upload, 2)
    
    return [agora, nome_maquina, cpu_percent, ram_total, ram_percent, disk.percent, download2, upload2]

def salvar_csv(dados):

    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        if file.tell() == 0:
            writer.writerow(headers)
        writer.writerow(dados)
    print(f"Dados salvos: {dados}")

if __name__ == "__main__":
    for _ in range(5):
        dados = coletar_dados()
        salvar_csv(dados)
        time.sleep(2)
