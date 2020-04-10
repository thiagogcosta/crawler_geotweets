
import tweepy
from twitter_accounts import accounts
from tweepy import Stream
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
import time 
from sqlalchemy.exc import ProgrammingError
from con_mongodb import Connection_Mongo
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

class Database():
    
    
    def __init__(self, local_host, porta, collection, local):
        self.con_mongo = Connection_Mongo(local_host, porta)
        self.db = self.con_mongo.create_db(collection)
        self.local = local
    
    def In_shape(self, df, shapes):
        
        in_shape = 0
        
        for sh in shapes.geometry:
            if(df.within(sh)):
                in_shape = 1
        return in_shape
        
    def setData(self, status):
        
        
        if status.coordinates:
            
            #--------------------------------------------------------------------------------------
            longitude = status.coordinates['coordinates'][0]
            latitude = status.coordinates['coordinates'][1]
            
            shape = gpd.read_file(r''+self.local+'Sao_Paulo_city_WGS84.shp')

            tweet = Point(longitude,latitude)
            
            if(self.In_shape(tweet, shape)):
                
                colunas = ['id_str','created','text','usuario','loc','latitude','longitude']
            
                df = pd.DataFrame(columns = colunas) 
                
                #print(status)
                
                ultima_posicao = len(df) + 1
                df.loc[ultima_posicao, 'id_str'] = status.id_str
                df.loc[ultima_posicao, 'created'] = status.created_at
                df.loc[ultima_posicao, 'text'] = status.text
                df.loc[ultima_posicao, 'usuario'] = status.user.screen_name
                df.loc[ultima_posicao, 'loc'] = status.user.location
                df.loc[ultima_posicao, 'longitude'] = longitude
                df.loc[ultima_posicao, 'latitude'] = latitude
                
                print(df)

                self.con_mongo.insert_collection_pandas(self.db,'tweets',[df])

                print('-------------------------------------------------------------')

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """

    def on_status(self, status):
        #print(status.text)
        
        try:
            local = '/home/thiago-costa/master_researcher/NewResearchCERTO/crawler_tweets/'
            aux_db = Database('localhost', '27017', 'crawler_insideSP_tweets',local)
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