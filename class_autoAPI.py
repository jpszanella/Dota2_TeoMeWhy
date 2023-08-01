# getProMatches -> parâmetro 
import datetime
import requests
import pandas as pd

class IngestorAPI():

    def __init__(self, url, to_stop): # url que bate e para
        self.url = url 
        self.to_stop = datetime.datetime.strptime(to_stop, "%Y-%m-%d")

    def get_resp(self, **kwargs): # recebe parâmetros e retorna uma response
        resp = requests.get(self.url, params=kwargs)
        return resp

    def save_data(self, data): # salvamento de dados
        path = "/mnt/datalake/dota/pro_matches/"
        df = spark.createDataFrame(data) # tansforma o dado em DataFrame - view
        df.coalesce(1).write.format("parquet").mode("append").save(path)
        # leitura do dado em RAW -> na hora de coletar todas as partidas
        # dados brutos e respectiva tipagem de todos eles -> alteração de metadados
    
    def get_and_save(self, **kwargs): # função de pegar o dado e ir pra o próximo
        resp = self.get_resp(**kwargs)
        data = resp.json()
        self.save_data(data)
        return data # retorna uma data, depois de salvar

    # pega a primeira partida a partir da mínima, assim como o início da partida
    def auto_process(self):
        data = self.get_and_save()
        df = pd.DataFrame(data)
        less_than_match_id = df['match_id'].min() # id miníma pela API
        min_start_time = datetime.datetime.fromtimestamp(df['start_time'].min())
        while min_start_time > self.to_stop:
            df = pd.DataFrame(self.get_and_save(less_than_match_id=less_than_match_id))
            less_than_match_id = df('match_id').min()
            min_start_time = datetime.datetime.fromtimestamp(df['start_time'].min())
          
url = "https://api.opendota.com/api/proMatches" 

dt_stop = dbutils.widgets.get("stop")

delay = int(dbutils.widgets.get("delay"))

dt_stop = datetime.datetime.strptime(dt_stop, "%Y-%m-%d") - datetime.timedelta(days=delay)
dt_stop = dt_stop.strftime("%Y-%m-%d")

ing = IngestorAPI(url, dt_stop)
ing.auto_process()
