# -*- coding: utf-8 -*-

import pymongo
import pandas as pd
from bson import ObjectId


class Connection_Mongo:  
    
    def __init__(self, server, mongodb_port):
        self.server = server
        self.mongodb_port = mongodb_port
        self.conexao = pymongo.MongoClient("mongodb://"+server+":"+mongodb_port+"/")
        
    
    #********CREATE DATABASE********
    def create_db(self,name):
        
        mydb = self.conexao[name]
        
        print('Base de Dados carregada!')
        
        return mydb
    
    #********DROP DATABASE********
    def drop_db(self,name):
    
        try:
            self.conexao.drop_database(name)
            
            print('Base de Dados deletada!')
            
        except ValueError as e:
            print('Erro:', e)
            
        return(1)
        
        
    #********CREATE Collection********
    def create_collection(self, mydb, name_collection):
        
        try:
            mycol = mydb[name_collection]
            
            print('Coleção criada!')
            
        except ValueError as e:
            print('Erro:', e)
            
        return(mycol)
        
     #********GET Collection********
    def get_collection(self, mydb, name_collection, query, remove_id):
        
        try:
            mycol = mydb[name_collection]
            
            cursor = mycol.find(query)
            
            df =  pd.DataFrame(list(cursor))
            
            if remove_id:
                del df['_id']

            
            print('Coleção criada!')
            
        except ValueError as e:
            print('Erro:', e)
            
        return(df)
    
    #********INSERT ONE Collection********
    def insert_one_collection(self, mydb, name_collection, vector_name, vector_data):
        
        try:
            
            if len(vector_name) == len(vector_data):
            
                mycol = mydb[name_collection]
            
                df = pd.DataFrame(columns = vector_name)
                
                index_df = len(df) + 1
                
                df.loc[index_df] = vector_data
                
                mycol.insert_many(df.to_dict('records'))
                
                print('Informação inserida na coleção com sucesso!')
                
            else:
                
                print('Impossível inserir esta informação na coleção')
        
        except ValueError as e:
            print('Erro:', e)
        
        return(1)
    
     #********DROP ONE Collection********
    def drop_one_collection(self, mydb, name_collection, identificador):
        
        try:
                
            mycol = mydb[name_collection]
            
            find = mycol.find_one({"_id": ObjectId(identificador)})
            
            mycol.delete_one({'_id': find['_id']})
            
            print('Informação removida com sucesso!')
    
        except ValueError as e:
            print('Erro:', e)
        
        return(1)    
    
    #********INSERT Collection PANDAS********
    def insert_collection_pandas(self, mydb, name_collection, vec_prec):
        
        try:
            
            for i in vec_prec:
                
                mydb[name_collection].insert_many(i.to_dict('records'))
            
                indexes = i.columns
                    
                mydb[name_collection].delete_many({indexes[0]: indexes[0]})
                
            print('Inserção de dataframe na coleção concluída!')
            
        except ValueError as e:
            print('Erro:', e)
        
        return(1)
        
    #********DELETE Collection********
    def drop_collection(self, mydb, name_collection):
        
        try:
            mydb.drop_collection(name_collection)
            
            print('Remoção de coleção concluída!')
            
        except ValueError as e:
            print('Erro:', e)
            
        return(1)
        
    #********CLEAR Collection********
    def clear_collection(self, mydb, name_collection):
        
        try:
            mydb[name_collection].delete_many({})
            
            print('Remoção de coleção concluída!')
            
        except ValueError as e:
            print('Erro:', e)
            
        return(1)
        
# =============================================================================
#x = Connection_Mongo('localhost', '27017')
#y = x.create_db('enoesocial')


#inmet = pd.read_csv('2016_11-2017_03-INMET-NEBUL-PRESS.csv', names = ['Estacao','Data','Hora','PressaoAtmEstacao','Nebulosidade'], sep=';')

#print(inmet)

#Connection_Mongo.clear_collection(y, 'inmet')

#Connection_Mongo.insert_collection_pandas(y, 'inmet', [inmet])

#prec1 = pd.read_csv('2692_SP_2016_11.csv', names = ['municipio', 'codEstacao', 'uf', 'nomeEstacao', 'latitude', 'longitude', 'datahora', 'valorMedida'], sep=';')
#prec2 = pd.read_csv('2692_SP_2016_12.csv', names = ['municipio', 'codEstacao', 'uf', 'nomeEstacao', 'latitude', 'longitude', 'datahora', 'valorMedida'], sep=';')
#prec3 = pd.read_csv('2692_SP_2017_1.csv', names = ['municipio', 'codEstacao', 'uf', 'nomeEstacao', 'latitude', 'longitude', 'datahora', 'valorMedida'], sep=';')
#prec4 = pd.read_csv('2692_SP_2017_2.csv', names = ['municipio', 'codEstacao', 'uf', 'nomeEstacao', 'latitude', 'longitude', 'datahora', 'valorMedida'], sep=';')
#prec5 = pd.read_csv('2692_SP_2017_3.csv', names = ['municipio', 'codEstacao', 'uf', 'nomeEstacao', 'latitude', 'longitude', 'datahora', 'valorMedida'], sep=';')
#
#vec_prec = [prec1,prec2, prec3, prec4, prec5]

#Connection_Mongo.insert_collection_pandas(y, 'cemaden', vec_prec)
#
# #Connection_Mongo.clear_collection(y, 'inmet')
# 
# #y.create_collection('estacoes_sp')
# #y.create_collection('cemaden')
# #y.create_collection('cge')
# #for i in vec_prec:   
# #    Connection_Mongo.insert_collection_pandas(y, 'inmet', i)
# 
#Connection_Mongo.clear_collection(y, 'inmet')
# 
# vec_name = ['municipio','codEstacao','uf','nomeEstacao','latitude','longitude','datahora','valorMedida']
# vec_data = ['São Paulo','355030801A','SP','Jardim Paulistano','-46,751','-23,582','2016-11-01 00:00:00.0','0']
# 
# #Connection_Mongo.insert_one_collection(y, 'inmet', vec_name, vec_data)
# 
# Connection_Mongo.drop_one_collection(y, 'inmet', '5d5ebbee06aa53383c331fe0')
# 
# #print(y.list_collections)
# =============================================================================

