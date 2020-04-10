# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 16:00:59 2019

@author: usuario
"""

from shapely.geometry import Point

class InsideShape:
    
    def In_shape(self, df, shapes):
        
        in_shape = 0
        
        for sh in shapes.geometry:
            if(df.within(sh)):
                in_shape = 1
        return in_shape
    
    def __init__(self,data,shape):
        
        data['inside'] = 0
        
        cont_tweet = 0
        
        list_index = data.index.values.tolist()
        
        while(cont_tweet < len(list_index)):
        
            df = Point(data.loc[list_index[cont_tweet]]['lon'],data.loc[list_index[cont_tweet]]['lat'])
            
            if(self.In_shape(df, shape)):
                data.loc[list_index[cont_tweet],'inside'] = 1
            else:
                data.loc[list_index[cont_tweet],'inside'] = 0
                
            cont_tweet +=1
            
        data = data[data['inside'] != 0]
        
        data = data.drop('inside', 1)
        
        data = data.reset_index(drop=True)
        
        self.data = data

