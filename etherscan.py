# -*- coding: utf-8 -*-
"""
Created on Wed Mar 07 17:42:47 2018
py-etherscan-api py2.7
@author: zch
"""

import urllib
import urllib2
import json
import collections
import pandas as pd
import time
import os
import re

def Get(url):
    req = urllib2.Request(url)
    import ssl    
    res_data = urllib2.urlopen(req,context=ssl._create_unverified_context())
    res = res_data.read()#.decode('utf-8')
    return json.loads(res)


class Etherscan(object):
    # Constants
    PREFIX = 'https://api.etherscan.io/api?'
    MODULE = 'module='
    ACTION = '&action='
    CONTRACT_ADDRESS = '&contractaddress='
    FROM_BLOCK = '&fromBlock='
    TO_BLOCK = '&toBlock='
    ADDRESS = '&address='
    TOPIC = '&topic'
    OFFSET = '&offset='
    PAGE = '&page='
    SORT = '&sort='
    BLOCK_TYPE = '&blocktype='
    TO = '&to='
    VALUE = '&value='
    DATA = '&data='
    POSITION = '&='
    HEX = '&hex='
    GAS_PRICE = '&gasPrice='
    GAS = '&gas='
    START_BLOCK = '&startblock='
    END_BLOCK = '&endblock='    
    BLOCKNO = '&blockno='
    TXHASH = '&txhash='
    TAG = '&tag='
    BOOLEAN = '&boolean='
    INDEX = '&index='
    API_KEY = '&apikey='

    url_dict = {}
    def __init__(self,address,api_key=''):
#        self.__url = 'https://api.etherscan.io/api?'
        self.__apikey = api_key
        self.url_dict = collections.OrderedDict([
            (self.MODULE, ''),
            (self.ACTION, ''),
            (self.CONTRACT_ADDRESS, ''),
            (self.FROM_BLOCK, ''),
            (self.TO_BLOCK, ''),
            (self.ADDRESS, ''),
            (self.TOPIC,''),
            (self.OFFSET, ''),
            (self.PAGE, ''),
            (self.SORT, ''),
            (self.BLOCK_TYPE, ''),
            (self.TO, ''),
            (self.VALUE, ''),
            (self.DATA, ''),
            (self.POSITION, ''),
            (self.HEX, ''),
            (self.GAS_PRICE, ''),
            (self.GAS, ''),
            (self.START_BLOCK, ''),
            (self.END_BLOCK, ''),
            (self.BLOCKNO, ''),
            (self.TXHASH, ''),
            (self.TAG, ''),
            (self.BOOLEAN, ''),
            (self.INDEX, ''),
            (self.API_KEY, api_key)]
        )        
        self.check_and_get_api()
        if (len(address) > 20) and (type(address) == list):
            print("Etherscan only takes 20 addresses at a time")
            quit()
        elif (type(address) == list) and (len(address) <= 20):
            self.url_dict[self.ADDRESS] = ','.join(address)
        else:
            self.url_dict[self.ADDRESS] = address
            
    def check_and_get_api(self):
        if self.__apikey:  # Check if api_key is empty string
            pass
        else:
            self.__apikey = input('Please type your EtherScan.io API key: ')
            
    def build_url(self):
        self.url = self.PREFIX + ''.join([param + val if val else '' for param, val in self.url_dict.items()])
        
    def get_balance(self):
        self.url_dict[self.MODULE] = 'account'
        self.url_dict[self.ACTION] = 'balance'
        self.url_dict[self.TAG] = 'latest'
        self.build_url()
        req = Get(self.url)
        return req['result']

    def get_balance_multiple(self):
        self.url_dict[self.MODULE] = 'account'
        self.url_dict[self.ACTION] = 'balancemulti'
        self.url_dict[self.TAG] = 'latest'
        self.build_url()
        req = Get(self.url)
        return req['result']
    
    def get_order_event_logs(self, fromblock= 1, toblock=100000, topic=''):
        """
        only the first 1000 results are return. So toblock-fromblock=1000
        """
        self.url_dict[self.MODULE] = 'logs'
        self.url_dict[self.ACTION] = 'getlogs'
        self.url_dict[self.FROM_BLOCK] = str(fromblock)
        self.url_dict[self.TO_BLOCK] = str(toblock)
        self.url_dict[self.TOPIC] = topic

        self.build_url()
        req = Get(self.url)
         
        if req['result']:
            req = pd.DataFrame(req['result'])
            req=req.replace('0x','0')
    
            req[['blockNumber','timeStamp', u'gasPrice', u'gasUsed',u'logIndex',u'transactionIndex']] = \
               req[['blockNumber','timeStamp', u'gasPrice', u'gasUsed', u'logIndex',u'transactionIndex']].applymap(lambda x: long(x,16))
            req['timeStamp'] = req.timeStamp.apply(lambda x:pd.to_datetime(time.asctime(time.gmtime(x))))
            req = req.set_index('timeStamp')
#            req.gasPrice *= 10**(-18)#conver to Ether
            req['from_address'] = req.topics.apply(lambda x:x[1][:2]+x[1][26:])
            req['to_address'] = req.data.apply(lambda x:'0x'+x[2:][64*0:64*1][24:])
#            import pdb
#            pdb.set_trace()     
            
            req['from_token'] = req.data.apply(lambda x:'0x'+x[2:][64*1:64*2][24:])
            req['from_token_volume'] = req.data.apply(lambda x:long(x[2:][64*3:64*4],16)*10**(-18))
            req['to_token'] = req.data.apply(lambda x:'0x'+x[2:][64*2:64*3][24:])
            req['to_token_volume'] = req.data.apply(lambda x:long(x[2:][64*4:64*5],16)*10**(-18))
            
            # the order volume calculate by Ether
            weth_address = ['0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2','0x2956356cd2a2bf3202f771f50d3d14a367b48070']
            req['ether_volume'] = 0
            req['ether_volume'].loc[req.from_token==weth_address[0]] = req['from_token_volume'][req.from_token==weth_address[0]]
            req['ether_volume'].loc[req.to_token==weth_address[0]] = req['to_token_volume'][req.to_token==weth_address[0]]
            req['ether_volume'].loc[req.from_token==weth_address[1]] = req['from_token_volume'][req.from_token==weth_address[1]]
            req['ether_volume'].loc[req.to_token==weth_address[1]] = req['to_token_volume'][req.to_token==weth_address[1]]
            
    #        req['data_count'] = req.data.apply(lambda x:(len(x)-2)/64.)
    
            return req
        else:
            return req

    def get_deposit_event_logs(self, fromblock= 1, toblock=100000, topic=''):
        """
        only the first 1000 results are return. So toblock-fromblock=1000
        """
        self.url_dict[self.MODULE] = 'logs'
        self.url_dict[self.ACTION] = 'getlogs'
        self.url_dict[self.FROM_BLOCK] = str(fromblock)
        self.url_dict[self.TO_BLOCK] = str(toblock)
        self.url_dict[self.TOPIC] = topic

        self.build_url()
        req = Get(self.url)
         
        if req['result']:
            req = pd.DataFrame(req['result'])
            req=req.replace('0x','0')
    
            req[['blockNumber','timeStamp', u'gasPrice', u'gasUsed',u'logIndex',u'transactionIndex']] = \
               req[['blockNumber','timeStamp', u'gasPrice', u'gasUsed', u'logIndex',u'transactionIndex']].applymap(lambda x: long(x,16))
            req['timeStamp'] = req.timeStamp.apply(lambda x:pd.to_datetime(time.asctime(time.gmtime(x))))            
            req = req.set_index('timeStamp')    
            req['weth_volume'] = req.data.apply(lambda x:long(x[2:][64*0:64*1],16)*10**(-18))
            req['from_address'] = req.topics.apply(lambda x:x[1][:2]+x[1][26:])
            
            return req
        else:
            return req            
              
            
    def get_transaction_block(self, startblock=1, endblock=10000000, sort='asc', internal=False):
        """
        Get a page of transactions, each transaction returns list of dict with keys:
            nonce
            hash
            cumulativeGasUsed
            gasUsed
            timeStamp
            blockHash
            value (in wei)
            input
            gas
            isInternalTx
            contractAddress
            confirmations
            gasPrice
            transactionIncex
            to
            from
            isError
            blockNumber
        sort options:
            'asc' -> ascending order
            'des' -> descending order
        internal options:
            True  -> Gets the internal transactions of a smart contract
            False -> (default) get normal external transactions
        """
        self.url_dict[self.MODULE] = 'account'
        if internal:
            self.url_dict[self.ACTION] = 'txlistinternal'
        else:
            self.url_dict[self.ACTION] = 'txlist'
        self.url_dict[self.START_BLOCK] = str(startblock)
        self.url_dict[self.END_BLOCK] = str(endblock)
        self.url_dict[self.SORT] = sort
        self.build_url()
        req = Get(self.url)
        return req['result']
    
    def get_transaction_page(self, page=1, offset=10000, sort='asc', internal=False):
        """
        Get a page of transactions, each transaction returns list of dict with keys:
            nonce
            hash
            cumulativeGasUsed
            gasUsed
            timeStamp
            blockHash
            value (in wei)
            input
            gas
            isInternalTx
            contractAddress
            confirmations
            gasPrice
            transactionIncex
            to
            from
            isError
            blockNumber
        sort options:
            'asc' -> ascending order
            'des' -> descending order
        internal options:
            True  -> Gets the internal transactions of a smart contract
            False -> (default) get normal external transactions
        """
        self.url_dict[self.MODULE] = 'account'
        if internal:
            self.url_dict[self.ACTION] = 'txlistinternal'
        else:
            self.url_dict[self.ACTION] = 'txlist'
        self.url_dict[self.PAGE] = str(page)
        self.url_dict[self.OFFSET] = str(offset)
        self.url_dict[self.SORT] = sort
        self.build_url()
        req = Get(self.url)
        return req['result']

    def get_all_transactions(self, offset=10000, sort='asc', internal=False):
        self.url_dict[self.MODULE] = 'account'
        if internal:
            self.url_dict[self.ACTION] = 'txlistinternal'
        else:
            self.url_dict[self.ACTION] = 'txlist'
        self.url_dict[self.PAGE] = str(1)
        self.url_dict[self.OFFSET] = str(offset)
        self.url_dict[self.SORT] = sort
        self.build_url()

        trans_list = []
        while True:
            self.build_url()
            req = Get(self.url)
            if "No transactions found" in req['message']:
                print("Total number of transactions: {}".format(len(trans_list)))
                self.page = ''
                return trans_list
            else:
                trans_list += req['result']
                # Find any character block that is a integer of any length
                page_number = re.findall(r'[1-9](?:\d{0,2})(?:,\d{3})*(?:\.\d*[1-9])?|0?\.\d*[1-9]|0', self.url_dict[self.PAGE])
                print("page {} added".format(page_number[0]))
                self.url_dict[self.PAGE] = str(int(page_number[0]) + 1)

    def get_blocks_mined_page(self, blocktype='blocks', page=1, offset=10000):
        """
        Get a page of blocks mined by given address, returns list of dict with keys:
            blockReward (in wei)
            blockNumber
            timeStamp
        blocktype options:
            'blocks' -> full blocks only
            'uncles' -> uncles only
        """
        self.url_dict[self.MODULE] = 'account'
        self.url_dict[self.ACTION] = 'getminedblocks'
        self.url_dict[self.BLOCK_TYPE] = blocktype
        self.url_dict[self.PAGE] = str(page)
        self.url_dict[self.OFFSET] = str(offset)
        self.build_url()
        req = Get(self.url)
        return req['result']

    def get_all_blocks_mined(self, blocktype='blocks', offset=10000):
        self.url_dict[self.MODULE] = 'account'
        self.url_dict[self.ACTION] = 'getminedblocks'
        self.url_dict[self.BLOCK_TYPE] = blocktype
        self.url_dict[self.PAGE] = str(1)
        self.url_dict[self.OFFSET] = str(offset)
        blocks_list = []
        while True:
            self.build_url()
            req = Get(self.url)
            print(req['message'])
            if "No transactions found" in req['message']:
                print("Total number of blocks mined: {}".format(len(blocks_list)))
                return blocks_list
            else:
                blocks_list += req['result']
                # Find any character block that is a integer of any length
                page_number = re.findall(r'[1-9](?:\d{0,2})(?:,\d{3})*(?:\.\d*[1-9])?|0?\.\d*[1-9]|0', self.url_dict[self.PAGE])
                print("page {} added".format(page_number[0]))
                self.url_dict[self.PAGE] = str(int(page_number[0]) + 1)

    def get_internal_by_hash(self, tx_hash=''):
        """
        Currently not implemented
        :return:
        """
        pass

    def update_transactions(self, address, trans):
        """
        Gets last page of transactions (last 10k trans) and updates current trans book (book)
        """
        pass  
    
    def get_abi(self):
        self.url_dict[self.MODULE] = 'contract'
        self.url_dict[self.ACTION] = 'getabi'
        self.build_url()
        req = Get(self.url)
        
        return json.loads(req['result'])
    
    
if __name__=='__main__':
#    key = '94G4QVTQCZ95WQJ4IAE54T7CSU895H1W97'
    key = 'C83H9338FT82Z7IAKFG9MPEBEDF75XUS2S'
#    address = ['0xddbd2b932c763ba5b1b7ae3b362eac3e8d40121a','0x281055Afc982d96fAB65b3a49cAc8b878184Cb16']
#    address = address[0]
    address = '0x12459c951127e0c374ff9105dda097662a027093'#0x contract
    ddex_address = '0xe269e891a2ec8585a378882ffa531141205e92e9'#ddex 
    topic0 = '0x0d0b9391970d9a25552f37d436d2aae2925e2bfe1b2a923754bada030c498cb3'#batchFillOrKillOrder,fillOrder
    topic2 = ['0x000000000000000000000000e269e891a2ec8585a378882ffa531141205e92e9','0x0000000000000000000000000000000000000000000000000000000000000000']
#    topic2 = '0x000000000000000000000000e269e891a2ec8585a378882ffa531141205e92e9'#Fee Recipient
#    topic2 = '0x0000000000000000000000000000000000000000000000000000000000000000'
#    topic = '0='+topic0+'&topic0_2_opr=and&topic2='+topic2
#    topic = '0='+topic0    

    f = Etherscan(address=address,api_key=key)
    try:
        #all history data of ddex
        ddex_history = pd.read_csv('ddex_history.csv',index_col=0,parse_dates=[0])
#        ddex_history[[u'from_token_volume', u'to_token_volume', u'ether_volume']] = \
#                    ddex_history[[u'from_token_volume', u'to_token_volume', u'ether_volume']].astype(float)
    except:
        ddex_history = pd.DataFrame()
        
    latest_block = 5249527
    TheFirstOrderBlock = 4806907#4806907:the block of first order for ddex
   
    # no local data, then get the data from the very first
    if ddex_history.shape[0]<=0:  
         
        for i in range(2):
            topic = '0='+topic0+'&topic0_2_opr=and&topic2='+topic2[i]
            fromblock = TheFirstOrderBlock
            if i==0:#Fee Recipient is ddex
                while True:       
                    event_logs = f.get_order_event_logs(fromblock=fromblock, toblock=latest_block, topic=topic)
                    if type(event_logs) != pd.core.frame.DataFrame:
                        break
                    if event_logs.shape[0]<1000:
                        fromblock = latest_block
                    else:
                        fromblock = event_logs.blockNumber[-1]
                    ddex_history = ddex_history.append(event_logs)
                    print fromblock
                    if fromblock >= latest_block: 
                        break
            else:##Fee Recipient is no one, filtered by the data0(taker)
                while True:
                    event_logs = f.get_order_event_logs(fromblock=fromblock, toblock=latest_block, topic=topic)
                    if (type(event_logs) != pd.core.frame.DataFrame):
                        break
                    if event_logs.shape[0]<1000:
                        fromblock = latest_block
                    else:
                        fromblock = event_logs.blockNumber[-1]
                    event_logs = event_logs[event_logs.to_address==ddex_address]
                    ddex_history = ddex_history.append(event_logs)
                    print fromblock
                    if fromblock >= latest_block: 
                        break
                        
        
    else:
        last_block = ddex_history.blockNumber[-1]
        for i in range(2):
            topic = '0='+topic0+'&topic0_2_opr=and&topic2='+topic2[i]
            fromblock = last_block
            if i==0:#Fee Recipient is ddex
                while True:       
                    
                    event_logs = f.get_order_event_logs(fromblock=fromblock, toblock=latest_block, topic=topic)
                    if type(event_logs) != pd.core.frame.DataFrame:
                        break
                    if event_logs.shape[0]<1000:
                        fromblock = latest_block
                    else:
                        fromblock = event_logs.blockNumber[-1]
                    ddex_history = ddex_history.append(event_logs)
                    print fromblock
                    if fromblock >= latest_block: 
                        break
            else:##Fee Recipient is no one, filtered by the data0(taker)
                while True:
                    event_logs = f.get_order_event_logs(fromblock=fromblock, toblock=latest_block, topic=topic)
                    if type(event_logs) != pd.core.frame.DataFrame:
                        break
                    if event_logs.shape[0]<1000:
                        fromblock = latest_block
                    else:
                        fromblock = event_logs.blockNumber[-1]
                    event_logs = event_logs[event_logs.to_address==ddex_address]
                    ddex_history = ddex_history.append(event_logs)
                    print fromblock
                    if fromblock >= latest_block: 
                        break

    ddex_history = ddex_history.sort_index().drop_duplicates([u'address', u'blockNumber', u'data', u'gasPrice', u'gasUsed',
       u'logIndex', u'transactionHash', u'transactionIndex', u'from_address', u'to_address', u'from_token', u'to_token'])        
    
    ddex_history.to_csv('ddex_history.csv', date_format='%Y-%m-%d %H:%M:%S')            
    
    #ddex statistic data
    try:
        ddex_sts = pd.read_csv('ddex_sts.csv',index_col=0,parse_dates=[0])
        
    except:
        ddex_sts = pd.DataFrame()
        
    if ddex_sts.shape[0] <= 0:
        ddex_history_date = ddex_history.groupby(ddex_history.index.date)
        ddex_sts['volume'] = ddex_history_date.ether_volume.sum()#*10**(-18)
        ddex_sts['order_counts'] = ddex_history_date.topics.count()
        ddex_sts['wallet_counts'] = ddex_history_date.from_address.apply(lambda x:len(x.unique()))
        
    else:
        last_date = ddex_sts.index[-1]
        ddex_history_date = ddex_history[last_date:].groupby(ddex_history[last_date:].index.date)
        ddex_sts_ = pd.DataFrame()
        ddex_sts_['volume'] = ddex_history_date.ether_volume.sum()#*10**(-18)
        ddex_sts_['order_counts'] = ddex_history_date.topics.count()
        ddex_sts_['wallet_counts'] = ddex_history_date.from_address.apply(lambda x:len(x.unique()))
        ddex_sts = ddex_sts[:-1].append(ddex_sts_)
        
    ddex_sts.index = pd.to_datetime(ddex_sts.index,format='%Y-%m-%d')
    ddex_sts.index.name = 'date'
    ddex_sts.to_csv('ddex_sts.csv')
    
    #deposit
    topic0 = '0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c'
    topic1 = '0x000000000000000000000000e269e891a2ec8585a378882ffa531141205e92e9'
    wrap_eth_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    topic = '0=' +topic0+'&topic0_1_opr=and&topic1=' +topic1  
    topic = '0=' +topic0
    deposit = Etherscan(address=wrap_eth_address,api_key=key)
    deposit_logs = deposit.get_deposit_event_logs(toblock=latest_block, topic=topic)
    
  
        
        
        


    
    
    
    
    
    
    