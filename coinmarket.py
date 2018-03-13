# -*- coding: utf-8 -*-
"""
Created on Mon Mar 12 09:59:44 2018

@author: zch
"""

from bs4 import BeautifulSoup  
import urllib
import urllib2
import pandas as pd
import datetime
import re
from decimal import Decimal
import pickle

def Get(url):
    req = urllib2.Request(url)
    import ssl    
    res_data = urllib2.urlopen(req,context=ssl._create_unverified_context())
    res = res_data.read()#.decode('utf-8')
    return res

#get the table data on the page of 24 hour volume rankings(Exchange)
url = 'https://coinmarketcap.com/exchanges/'
coin = Get(url)
soup = BeautifulSoup(coin,'lxml')
table_tag= soup.select('table')[0]
tab_data = [[item.text for item in row_data.select("th,td")]
                for row_data in table_tag.select("tr")]
#url_data = {item.text:'https://coinmarketcap.com'+item.get('href') 
#                for row_data in table_tag.select("tr") for item in row_data.select("a")}

#Get the url for the pair on the page of 24 hour volume rankings(Exchange)
url_data = {}
currency_url = 'https://coinmarketcap.com'
for row_data in table_tag.select("tr"):
    for item in row_data.select("a"):
        if '/' in item.text:
            url_data[item.text] = currency_url
        else:
            currency_url = 'https://coinmarketcap.com'+item.get('href')   
                        
def make_hyperlink(x,url_data=url_data):
    try:
        return '=HYPERLINK("%s","%s")'%(url_data[x],x)
    except:
        return '0' 

# Get all the exchanges url            
exchanges_url = {}
for row_data in table_tag.select("tr"):
    for item in row_data.select("a"):
        if 'exchanges' in item.get('href') and item.text!='View More':
            exchanges_url[item.text] = 'https://coinmarketcap.com'+item.get('href')
     
#Make the table data into a dict of each exchange DataFrame data         
meta_data = {}
start = 0
for i in range(len(tab_data)):
    if '.' in tab_data[i][0] and len(tab_data[i])==1:
        if start == 0:
            start = 1
        else:
            exchange = tab_data[start-1][0].split('. ')[1]
            meta_data[exchange] = pd.DataFrame(tab_data[start+1:i],columns=[u'#', u'Currency', u'Pair', u'Volume (24h)', u'Price', u'Volume (%)'])
            meta_data[exchange] = meta_data[exchange].set_index('#')
            meta_data[exchange] = meta_data[exchange].fillna('0')
            #some exchange has no Total data
            if 'Total' in meta_data[exchange].index:
                meta_data[exchange].loc['Total',u'Volume (24h)'] = meta_data[exchange].loc['Total','Currency']
                meta_data[exchange][[u'Volume (24h)', u'Price']] = meta_data[exchange][[u'Volume (24h)', u'Price']].applymap(lambda x:Decimal(re.sub(r'[^\d.]','',x)))
            else:
                meta_data[exchange][[u'Volume (24h)', u'Price']] = meta_data[exchange][[u'Volume (24h)', u'Price']].applymap(lambda x:Decimal(re.sub(r'[^\d.]','',x)))
                meta_data[exchange].loc['Total',u'Volume (24h)'] = meta_data[exchange][u'Volume (24h)'].sum()
            #add hyperlink    
            meta_data[exchange]['Pair'] = meta_data[exchange]['Pair'].apply(lambda x:make_hyperlink(x))
            start = i+1
    elif i+1 == len(tab_data):
        exchange = tab_data[start-1][0].split('. ')[1]
        meta_data[exchange] = pd.DataFrame(tab_data[start+1:],columns=[u'#', u'Currency', u'Pair', u'Volume (24h)', u'Price', u'Volume (%)'])
        meta_data[exchange] = meta_data[exchange].set_index('#')
        meta_data[exchange] = meta_data[exchange].fillna('0')
        #some exchange has no Total data
        if 'Total' in meta_data[exchange].index:
            meta_data[exchange].loc['Total',u'Volume (24h)'] = meta_data[exchange].loc['Total','Currency']
            meta_data[exchange][[u'Volume (24h)', u'Price']] = meta_data[exchange][[u'Volume (24h)', u'Price']].applymap(lambda x:Decimal(re.sub(r'[^\d.]','',x)))
        else:
            meta_data[exchange][[u'Volume (24h)', u'Price']] = meta_data[exchange][[u'Volume (24h)', u'Price']].applymap(lambda x:Decimal(re.sub(r'[^\d.]','',x)))
            meta_data[exchange].loc['Total',u'Volume (24h)'] = meta_data[exchange][u'Volume (24h)'].sum()
        #add hyperlink
        meta_data[exchange]['Pair'] = meta_data[exchange]['Pair'].apply(lambda x:make_hyperlink(x))


       
exchanges = ['OKEx','Binance','Bitfinex','Huobi','Upbit']
n = 3 #the top n volume pair of exchange  

try:
    exchanges_active_markets = pickle.load(open('exchanges_active_markets.txt','rb'))  
except:
    exchanges_active_markets = {}


for exchange in exchanges:
    if exchange in meta_data.keys():
        new_pairs = {}
        if exchange in exchanges_active_markets:
            active_markets = 1
        else:
            exchanges_active_markets[exchange] = {}
            active_markets = 0
        exchange_page = BeautifulSoup(Get(exchanges_url[exchange]),'lxml').select('table')[0]
        for row_data in exchange_page.select("tr"):
            for item in row_data.select("a"):
                if '/' in item.text and item.text not in exchanges_active_markets[exchange]:
                    exchanges_active_markets[exchange][item.text] = currency_url
                    new_pairs[item.text] = currency_url                            
                                        
                else:
                    currency_url = 'https://coinmarketcap.com'+item.get('href')
        
        
        try: 
            #read history data
            data_hist = pd.read_csv(exchange+'.csv',index_col=[0],parse_dates=[0])
#            data_hist[[i for i in data_hist.columns if 'top' in i ]] = data_hist[[i for i in data_hist.columns if 'top' in i]].applymap(lambda x:make_hyperlink(x))
            data = pd.DataFrame(index=[datetime.datetime.utcnow()],columns=['total_volume_usd']+['top_'+str(i+1)+'_pair' for i in range(n)])            
            data['total_volume_usd'] = meta_data[exchange].loc['Total',u'Volume (24h)']
            #exist history active markets
            if active_markets:
                for n_new_pair in range(len(new_pairs)):
                    data['new_pair_'+str(n_new_pair)] = make_hyperlink(new_pairs.keys()[n_new_pair],url_data=new_pairs)                
            else:
                pass
            for j in range(n):
                if str(j+1) in meta_data[exchange].index: 
                   data['top_'+str(j+1)+'_pair'] = meta_data[exchange].loc[str(j+1),'Pair'] 
            if data_hist.index[-1].date() == data.index[-1].date():
                #the early new pair in the same day should be inherited 
                new_pairs_hist = data_hist.iloc[-1][[i for i in data_hist.columns if 'new_pair' in i]].dropna().values
                num_new_pair_data = len([i for i in data.columns if 'new_pair' in i])
                for i in range(len(new_pairs_hist)):
                    data['new_pair_'+str(i+num_new_pair_data)] = new_pairs_hist[i]
                data_hist = data_hist[:-1].append(data)
            else:
                data_hist = data_hist.append(data)
            
            data_hist.index.name = exchange
            data_hist.to_csv(exchange+'.csv', date_format='%Y-%m-%d %H:%M:%S')              
              
        except:
            #create new exchange sheet
            data = pd.DataFrame(index=[datetime.datetime.utcnow()],columns=['total_volume_usd']+['top_'+str(i+1)+'_pair' for i in range(n)])
            data['total_volume_usd'] = meta_data[exchange].loc['Total',u'Volume (24h)']
            #exist history active markets
            if active_markets:
                for n_new_pair in range(len(new_pairs)):
                    data['new_pair_'+str(n_new_pair)] = make_hyperlink(new_pairs[new_pairs.keys()[n_new_pair]],url_data=new_pairs)                
            else:
                pass
            for j in range(n):
                if str(j+1) in meta_data[exchange].index: 
                   data['top_'+str(j+1)+'_pair'] = meta_data[exchange].loc[str(j+1),'Pair']   
            data.index.name = exchange
#            data[[i for i in data.columns if 'top' in i ]] = data[[i for i in data.columns if 'top' in i]].applymap(lambda x:make_hyperlink(x))
            data.to_csv(exchange+'.csv', date_format='%Y-%m-%d %H:%M:%S')               
        
    else:
        print ('exchange name error: "%s" is not on the coinmarket list') %exchange 

pickle.dump(exchanges_active_markets,open('exchanges_active_markets.txt','wb'))
    

            

            









