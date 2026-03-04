import psutil
import csv
import platform
import time
import speedtest
from datetime import datetime

filename = "dados_maquina.csv"
headers = ["Data/Hora", "Maquina", "CPU_Uso_%", "RAM_Total_GB", "RAM_Uso_%", "Download_mbps", "Upload_mbps", "Temperatura"]

def coletar_dados():
    agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    nome_maquina = platform.node()
    cpu_percent = psutil.cpu_percent(interval=1)
    memoria = psutil.virtual_memory()
    ram_total = round(memoria.total / (1024**3), 2) 
    ram_percent = memoria.percent
    #st = speedtest.Speedtest()
    #download = st.download() / 10**6  
    #download2 = round(download, 2)
    #upload = st.upload() / 10**6
    #upload2 = round(upload, 2) , upload2, download2

    temps = psutil.sensors_temperatures()
    cpu_temp = 0
    gpu_temp = 0

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

    for nome_sensor, entradas in fans.items():
        for fan in entradas:
            label = fan.label.lower()
            if "cpu" in label:
                cpu_fan = fan.current
            elif "gpu" in label:
                gpu_fan = fan.current
    
    return [agora, nome_maquina, cpu_percent, ram_total, ram_percent, cpu_temp, gpu_temp, cpu_fan, gpu_fan]


def salvar_csv(dados):

    with open(filename, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        if file.tell() == 0:
            writer.writerow(headers)
        writer.writerow(dados)
    print(f"Dados salvos: {dados}")

if __name__ == "__main__":
    for _ in range(3):
        dados = coletar_dados()
        salvar_csv(dados)
        time.sleep(5)
