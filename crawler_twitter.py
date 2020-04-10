
import tweepy
from twitter_accounts import accounts
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import time 
from sqlalchemy.exc import ProgrammingError
from con_mongodb import Connection_Mongo
import pandas as pd

class Database():
    
    
    def __init__(self, local_host, porta, collection):
        self.con_mongo = Connection_Mongo(local_host, porta)
        self.db = self.con_mongo.create_db(collection)

    def setData(self, status):
        
        #--------------------------------KEY-WORDS---------------------------------------

        colunas = ['palavras-chave']

        kw = pd.DataFrame(columns = colunas)

        palavras_fenomenos = ["alagamento","alagado","alagada","alagando","alagou","alagar",
                            "chove","chova","chovia","chuva","chuvarada","chuvosa","chuvoso","chuvona","chuvinha","chuvisco","chuvendo",
                            "diluvio", "dilúvio", "enchente", "enxurrada",
                            "garoa","inundação","inundacao","inundada","inundado","inundar","inundam","inundou",
                            "temporal","temporais",
                            "tromba d'água"]

        for i in palavras_fenomenos:
            
            index = len(kw)
            
            kw.loc[index, 'palavras-chave'] = i

        #print(kw)
        #------------------------------------------------------------------------------
        
        cont = 0

        for i in range(len(kw)):

            if kw.loc[i]['palavras-chave'] in status.text.lower():

                print(kw.loc[i]['palavras-chave'])
                cont = 1
        
        if cont == 1:

            if status.coordinates:
                
                longitude = status.coordinates['coordinates'][0]
                latitude = status.coordinates['coordinates'][1]
                
                colunas = ['id_str','created','text','usuario','loc','latitude','longitude']
                df = pd.DataFrame(columns = colunas) 
                
                print(status)
                
                ultima_posicao = len(df) + 1
                df.loc[ultima_posicao, 'id_str'] = status.id_str
                df.loc[ultima_posicao, 'created'] = status.created_at
                df.loc[ultima_posicao, 'text'] = status.text
                df.loc[ultima_posicao, 'usuario'] = status.user.screen_name
                df.loc[ultima_posicao, 'loc'] = status.user.location
                df.loc[ultima_posicao, 'longitude'] = longitude
                df.loc[ultima_posicao, 'latitude'] = latitude
                
                print(df)

                self.con_mongo.insert_collection_pandas(self.db,'messages', [df])

                print('-------------------------------------------------------------')
        print('...')

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    def on_status(self, status):
        #print(status.text)
        if status.retweeted:
            return

        try:
            aux_db = Database('localhost', '27017', 'crawler_twitter')
            aux_db.setData(status)

        except ProgrammingError as err:
            print(err)

    def on_error(self, status_code):
        if status_code == 420:
            return False

if __name__ == '__main__':
    l = StdOutListener()

    app = accounts["social"]
    auth = OAuthHandler(app["api_key"], app["api_secret"])
    auth.set_access_token(app["token"], app["token_secret"])
    stream = Stream(auth,l)

    #GEOCOORDENADAS NORTE DE SÃO PAULO
    stream.filter(locations=[-46.95,-23.62,-46.28,-23.33],)

    #GEOCOORDENADAS SUL DE SÃO PAULO
    stream.filter(locations=[-46.95,-23.91,-46.28,-23.62],)