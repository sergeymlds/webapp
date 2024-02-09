# -*- coding: utf-8 -*-

"""
Routes and views for the flask application.
"""
from datetime import datetime, timedelta
from flask import render_template
from python_webapp_flask import app
from flask import Flask, request, jsonify
import pandas as pd
from collections import OrderedDict
import io
import json
import grequests
import os


from azure.storage.blob import BlockBlobService, PublicAccess, AppendBlobService
container_name = 'spardata'
key = os.environ.get('SECRET_KEY')
PLAN_HOST_IP = os.environ.get('PLAN_HOST_IP')
JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY')

blobstorageaccount = 'sparmlstorage'
counter = 0
indx = 0


from flask_jwt_extended import (
    JWTManager, jwt_required, create_access_token,
    jwt_refresh_token_required, create_refresh_token,
    get_jwt_identity
)

app.config['JWT_SECRET_KEY'] = JWT_SECRET_KEY
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = False


jwt = JWTManager(app)


def myconverter(o):
    if isinstance(o, datetime):
        return o.__str__()


@app.route('/')
@app.route('/home')
def home():
    """Renders the home page."""
    return render_template(
        'index.html',
        title='Home Page',
        year=datetime.now().year,
    )

@app.route('/login', methods=['POST'])
def login():
    username = request.json.get('username', None)
    password = request.json.get('password', None)
    if username != 'APP_USER' or password != 'SECRET_KEY_APP':
        return jsonify({"msg": "Bad username or password"}), 401

    # Use create_access_token() and create_refresh_token() to create our
    # access and refresh tokens
    ret = {
        'access_token': create_access_token(identity=username),
        'refresh_token': create_refresh_token(identity=username)
    }
    return jsonify(ret), 200


@app.route('/refresh', methods=['POST'])
@jwt_refresh_token_required
def refresh():
    current_user = get_jwt_identity()
    ret = {
        'access_token': create_access_token(identity=current_user)
    }
    return jsonify(ret), 200


@app.route('/contact')
def contact():
    """Renders the contact page."""
    return render_template(
        'contact.html',
        title=u'Контакты',
        #year=datetime.now().year,
        year=datetime.now().year,
        message=u'Контакты'
    )

@app.route('/about')
def about():
    """Renders the about page."""
    return render_template(
        'about.html',
        title='About',
        year=datetime.now().year,
        message='Your application description page.'
    )

@app.route("/check", methods=['POST'])
@jwt_required
def check():    
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['DateFact', 'Item', 'Qnty', 'PriceBase', 'SumTotal']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
        if len(data[data.duplicated(subset = ['DateFact', 'Item'])]) > 0:
            return jsonify({'status':'duplicate data by \'DateFact\' and \'Item\' fields'})
        
        try:
            data['DateFact'] = pd.to_datetime(data['DateFact'])
            data['Item'] = data['Item'].astype(int)
            data['Qnty'] = data['Qnty'].astype(float)
            data['PriceBase'] = data['PriceBase'].astype(float)
            data['SumTotal'] = data['SumTotal'].astype(float)
            max_date = data['DateFact'].max().date()
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})
            
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text(container_name, 
                                                 r'check/{}/{}.csv'.format(df_dict['store_id'],
                                                         max_date), output)
        return jsonify({'status':'success'})
    
    
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/online", methods=['POST'])
@jwt_required
def online():    
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['DateFact', 'Item', 'Qnty', 'PriceBase', 'SumTotal']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
        if len(data[data.duplicated(subset = ['DateFact', 'Item'])]) > 0:
            return jsonify({'status':'duplicate data by \'DateFact\' and \'Item\' fields'})
        
        try:
            data['DateFact'] = pd.to_datetime(data['DateFact'])
            data['Item'] = data['Item'].astype(int)
            data['Qnty'] = data['Qnty'].astype(float)
            data['PriceBase'] = data['PriceBase'].astype(float)
            data['SumTotal'] = data['SumTotal'].astype(float)
            max_date = data['DateFact'].max().date()
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})
            
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text(container_name, 
                                                 r'online/{}/{}.csv'.format(df_dict['store_id'],
                                                          max_date), output)
        return jsonify({'status':'success'})
    
    
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/stock", methods=['POST'])
@jwt_required
def stock():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #check columns names
        columns_cheking = (set(data.columns) == set(['DateEnd', 'Item', 'StockQuantity', 'StockCost', 'StockSale']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns order or names'})
        
        if len(data[data.duplicated(subset = ['DateEnd', 'Item'])]) > 0:
            return jsonify({'status':'duplicate data by \'DateEnd\' and \'Item\' fields'})
        
        try:
            data['DateEnd'] = pd.to_datetime(data['DateEnd'])
            data['Item'] = data['Item'].astype(int)
            data['StockQuantity'] = data['StockQuantity'].astype(float)
            data['StockCost'] = data['StockCost'].astype(float)
            data['StockSale'] = data['StockSale'].astype(float)
            max_date = data['DateEnd'].max().date()
        
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})        
        output = data.to_csv(index = False)
        
        block_blob_service.create_blob_from_text(container_name, r'stock/{}/{}.csv'.format(df_dict['store_id'], max_date), output)
        
        return jsonify({'status':'success'})
    
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})

    
@app.route("/actual", methods=['POST'])
@jwt_required
def actual():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #check columns names
        columns_cheking = (set(data.columns) == set(["Item", "DIVISION", "GROUP_NO", "DEPT", "CLASS", "SUBCLASS"]))
        if not columns_cheking:
            return jsonify({'status':'invalid columns order or names'})
        
        if len(data[data.duplicated(subset = ['Item'])]) > 0:
            return jsonify({'status':'duplicate data by \'Item\' field'})
        
        try:
            data['Item'] = data['Item'].astype(int)
            data['DIVISION'] = data['DIVISION'].astype(int)
            data['GROUP_NO'] = data['GROUP_NO'].astype(int)
            data['DEPT'] = data['DEPT'].astype(int)
            data['CLASS'] = data['CLASS'].astype(int)
            data['SUBCLASS'] = data['SUBCLASS'].astype(int)
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})
        
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text(container_name, r'actual/{}/actual.csv'.format(df_dict['store_id']), output)
        
        return jsonify({'status':'success'})
    
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})
    
    
@app.route("/discount", methods=['POST'])
@jwt_required
def discount():
    try:      
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        columns_cheking = (set(data.columns) == set(["store_id", "Item", "PromoTypeCode", 
                                                     "DateBegin", "DateEnd", 
                                                     "SalePriceBeforePromo", "SalePriceTimePromo"]))
        data['doc_id'] = df_dict['doc_id']
        now = datetime.now()
        data['uploading_date'] = now
        
        if not columns_cheking:
            return jsonify({'status':'invalid columns order or names'})
        
        if len(data[data.duplicated(subset = ["store_id", 'Item', 'DateBegin', 'DateEnd'])]) > 0:
            return jsonify({'status':'duplicate data by \'store_id\', \'Item\', \'DateBegin\' and \'DateEnd\'  fields'})

        try:
            data['store_id'] = data['store_id'].astype(int)
            data['Item'] = data['Item'].astype(int)
            data['PromoTypeCode'] = data['PromoTypeCode'].astype(str)
            data['DateBegin'] = pd.to_datetime(data['DateBegin'])
            data['DateEnd'] = pd.to_datetime(data['DateEnd'])
            data['SalePriceBeforePromo'] = data['SalePriceBeforePromo'].astype(float)
            data['SalePriceTimePromo'] = data['SalePriceTimePromo'].astype(float)
            data['doc_id'] = data['doc_id'].astype(str)
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns types'})


        output = data[["store_id", "Item", "PromoTypeCode", "DateBegin", 
                       "DateEnd", "SalePriceBeforePromo", "SalePriceTimePromo",
                       'doc_id', 'uploading_date']].to_csv(index = False)
                       
        block_blob_service.create_blob_from_text(container_name,
                        f'pwc/temp/{now}.csv', output)

        return jsonify({'status':'success'})
    
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})
    
    
@app.route("/discount1", methods=['POST'])
@jwt_required
def discount1():
    try:
        dics_map = {'000000012': 1,'000000013': 2,'000000014': 3,
         '000000022': 4,'000000020': 5,'000000001': 6,
         '000000004': 7,'000000017': 8,'000000016': 8,
         '000000018': 8,'000000019': 8,'000000015': 9,
         '000000007': 9,'000000002': 5,'000000021': 10}
        
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        columns_cheking = (set(data.columns) == set(["Item", "PromoTypeCode", 
                                                     "DateBegin", "DateEnd", 
                                                     "SalePriceBeforePromo", "SalePriceTimePromo"]))
        if not columns_cheking:
            return jsonify({'status':'invalid columns order or names'})
        
        if len(data[data.duplicated(subset = ['Item', 'DateBegin', 'DateEnd'])]) > 0:
            return jsonify({'status':'duplicate data by \'Item\', \'DateBegin\' and \'DateEnd\'  fields'})

        try:
            data['Item'] = data['Item'].astype(int)
            data['PromoTypeCode'] = data['PromoTypeCode'].map(dics_map)
            data['PromoTypeCode'] = data['PromoTypeCode'].astype(int)
            data['DateBegin'] = pd.to_datetime(data['DateBegin'])
            data['DateEnd'] = pd.to_datetime(data['DateEnd'])
            data['SalePriceBeforePromo'] = data['SalePriceBeforePromo'].astype(float)
            data['SalePriceTimePromo'] = data['SalePriceTimePromo'].astype(float)
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns types'})

#        actual = data[data['DateEnd'] >= datetime.now()]
#        actual_output = actual.to_csv(index = False)
        actual_output = data.to_csv(index = False)
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text(container_name,
                        r'discount/{}/discount.csv'.format(df_dict['store_id']), actual_output)

        return jsonify({'status':'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/prediction", methods=['POST'])
@jwt_required
def prediction():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        store_id = df_dict['store_id']
        try:
            with io.BytesIO() as input_blob:
                block_blob_service.get_blob_to_stream(container_name=container_name,
                                                  blob_name = r'predictions/{}/prediction.csv'.format(store_id),
                                                           stream = input_blob)
                input_blob.seek(0)
                df=pd.read_csv(input_blob)

            df_dict = df.to_dict(orient = 'split')
            df_dict.pop('index');
            df_dict.update({'store_id':store_id})
            df_dict1 = OrderedDict(store_id = df_dict['store_id'],
                                   columns = df_dict['columns'], data = df_dict['data'])
            #for key in ['store_id', 'columns', 'data']:
            #    df_dict1[key] = df_dict[key]

            return json.dumps(df_dict1)
        except:
            return json.dumps({'status':'something wrong, may be invalid store id'})

    except:
        return json.dumps({'status':'something wrong, check json stucture'})

@app.route("/markdown", methods=['POST'])
@jwt_required
def markdown():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)

        df_dict = request.get_json() 
        store_id = df_dict['store_id']  
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])     
        
        #check columns names
        columns_cheking = (set(data.columns) == set(["date", "Item", "NormalPrice", "Price", "Qnty"]))
        if not columns_cheking:
            return jsonify({'status':'invalid columns order or names'})        
        
        if len(data[data.duplicated(subset = ['Item', 'date'])]) > 0:
            return jsonify({'status':'duplicate data by \'Item\', \'date\' field'})
        try:
            data['date'] = pd.to_datetime(data['date'])
            data['Item'] = data['Item'].astype(int)
            data['NormalPrice'] = data['NormalPrice'].astype(float)
            data['Price'] = data['Price'].astype(float)
            data['Qnty'] = data['Qnty'].astype(float)
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})        
        
        max_date = data['date'].max().date()
        output = data[["date", "Item", "NormalPrice", "Price", "Qnty"]].to_csv(index = False, header=False)
        block_blob_service.create_blob_from_text(container_name, r'markdown/{}/{}.csv'.format(store_id, max_date), output)
        
        return jsonify({'status':'success'})
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})

@app.route("/price", methods=['POST'])
@jwt_required
def price():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key) 
        df_dict = request.get_json()

        store_id = df_dict['store_id']       
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])   
     
        #check columns names
        columns_cheking = (set(data.columns) == set(["date", "Item", "Price", "CodeRegPrcChg"]))
        if not columns_cheking:
            return jsonify({'status':'invalid columns order or names'})        
        
#        if len(data[data.duplicated(subset = ['date', 'Item'])]) > 0:
#            return jsonify({'status':'duplicate data by \'date\', \'Item\' fields'})
        
        try:
            data['Item'] = data['Item'].astype(int)
            data['date'] = pd.to_datetime(data['date'])
            data['Price'] = data['Price'].astype(float)
            data['CodeRegPrcChg'] = data['CodeRegPrcChg'].astype(int)
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})

        max_date = data['date'].max().date()        
        output = data[["date", "Item", "Price",  "CodeRegPrcChg"]].to_csv(index = False, header=False)
        block_blob_service.create_blob_from_text(container_name, r'price_history/{}/{}.csv'.format(store_id, max_date), output)
        
        return jsonify({'status':'success'})
    except:
        return jsonify({'status':'error'})

urls = [
    f'{PLAN_HOST_IP}/nopromo',
    f'{PLAN_HOST_IP}/promo'
]

@app.route("/promo", methods=['POST'])
def promo():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
                                       
        df_dict = request.get_json() 
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        # сохраняем историю запросов
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('promo', 
                                                 r'promo_query_history/{}.csv'.format(datetime.now()), output)   
        
        columns_cheking = (set(data.columns) == set(["Item", "Analog", "ObjCode", "DateBegin", "DateEnd", "DateFinal", "PromoTypeCode", "SalePriceBeforePromo", "SalePriceTimePromo", "NotInMatrix"]))
        if not columns_cheking:
            return jsonify({'status':'invalid columns order or names'})        
        
        if len(data[data.duplicated(subset = ['Item', 'ObjCode', 'DateBegin', 'DateEnd', 'DateFinal'])]) > 0:
            return jsonify({'status':'duplicate data by \'Item\', \'ObjCode\', \'DateBegin\', \'DateEnd\', \'DateFinal\' field'})
        try:
            data['Item'] = data['Item'].astype(int)
            data['Analog'] = data['Analog'].astype(float)
            data['ObjCode'] = data['ObjCode'].astype(int)
            data['DateBegin'] = pd.to_datetime(data['DateBegin'])
            data['DateEnd'] = pd.to_datetime(data['DateEnd'])
            data['DateFinal'] = pd.to_datetime(data['DateFinal'])
            data['SalePriceBeforePromo'] = data['SalePriceBeforePromo'].astype(float)
            data['SalePriceTimePromo'] = data['SalePriceTimePromo'].astype(float)
            data['NotInMatrix'] = data['NotInMatrix'].astype(int)
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})        
        
        #if (data['DateBegin'].min()<datetime.now().date()):
        #    return jsonify({'status':"DateBegin must be greater than today's date"})

        #if data[['SalePriceBeforePromo', 'SalePriceTimePromo']].min().min() <= 0:
        #    return jsonify({'status':"Item's prices must be greater than 0"})
        
        #data['dPrice'] = (data['SalePriceBeforePromo'] - data['SalePriceTimePromo'])/data['SalePriceBeforePromo']
    
        #if len(data[data['dPrice'] <= 0]) > 0:
        #        return jsonify({'status':'SalePriceTimePromo must be less than SalePriceBeforePromo'})

        data.loc[data['SalePriceBeforePromo'] < data['SalePriceTimePromo'], 'SalePriceBeforePromo'] = data.loc[data['SalePriceBeforePromo'] < data['SalePriceTimePromo'], 'SalePriceTimePromo']

        to_dict = data[["Item", "Analog", "ObjCode", "DateBegin", "DateEnd", "DateFinal", "PromoTypeCode", "SalePriceBeforePromo", "SalePriceTimePromo", "NotInMatrix"]].to_dict(orient = 'split')
        to_dict.pop('index')
        
        try:
            access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1NTk0ODcxNjIsIm5iZiI6MTU1OTQ4NzE2MiwianRpIjoiMzI4MmNiODUtYTUxZS00YTRhLTg1OGQtYTNkN2VkODY2ZTBmIiwiaWRlbnRpdHkiOiJzcGFyIiwiZnJlc2giOmZhbHNlLCJ0eXBlIjoiYWNjZXNzIn0.n0YvNuFeHuyanyjZdEJQ9zHox6P2F9EETetIA1Eylgw'
            headers = {'content-type': 'application/json', "Authorization": "Bearer {}".format(access_token)}
                        
            rs = (grequests.post(u, data=json.dumps(to_dict, default = myconverter), headers=headers) for u in urls)
            
            df = pd.DataFrame()
            for r in grequests.map(rs):
                to_concat = pd.DataFrame(data = json.loads(r.text)['data'], columns = json.loads(r.text)['columns'])
                df = pd.concat([df, to_concat], axis=1)
                
            df = df.loc[:,~df.columns.duplicated()]
            print(df)
        
            to_dict = df[['ObjCode', 'Item', 'BeforePromo', 'OnPromo', 'AfterPromo']].to_dict(orient = 'split')
            to_dict.pop('index')
            to_dict.update({'status':'success'})
            return jsonify(to_dict)
            
        except Exception as e:
            print(e)
            return jsonify({'status':'No access to remote server'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})
    
urls_lgbm = [
    f'{PLAN_HOST_IP}/nopromo',
    f'{PLAN_HOST_IP}/promo',
    f'{PLAN_HOST_IP}/promo_lightgbm'
]

block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='promo',
                                              blob_name = 'item_dict.csv',
                                              stream = input_blob)
    input_blob.seek(0)
    itemdict=pd.read_csv(input_blob)
season_items = [4231, 4237, 5002, 7335, 6088, 5003, 4492, 6075, 6095, 8538, 4539, 7341, 5001, 4240, 4652, 4653, 4651, 4233, 4234, 4235, 6196, 4236, 4863, 4860, 4861, 4862, 9335, 4239]

@app.route("/promo_lgbm", methods=['POST'])
def promo_lgbm():
    global itemdict, season_items
    try: 
        df_dict = request.get_json() 
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])     
        
        columns_cheking = (set(data.columns) == set(["Item", "Analog", "ObjCode", "DateBegin", "DateEnd", "DateFinal", "PromoTypeCode", "SalePriceBeforePromo", "SalePriceTimePromo", "NotInMatrix"]))
        if not columns_cheking:
            return jsonify({'status':'invalid columns order or names'})        
        
        if len(data[data.duplicated(subset = ['Item', 'ObjCode', 'DateBegin', 'DateEnd', 'DateFinal'])]) > 0:
            return jsonify({'status':'duplicate data by \'Item\', \'ObjCode\', \'DateBegin\', \'DateEnd\', \'DateFinal\' field'})
        try:
            data['Item'] = data['Item'].astype(int)
            data['Analog'] = data['Analog'].astype(float)
            data['ObjCode'] = data['ObjCode'].astype(int)
            data['DateBegin'] = pd.to_datetime(data['DateBegin'])
            data['DateEnd'] = pd.to_datetime(data['DateEnd'])
            data['DateFinal'] = pd.to_datetime(data['DateFinal'])
            data['SalePriceBeforePromo'] = data['SalePriceBeforePromo'].astype(float)
            data['SalePriceTimePromo'] = data['SalePriceTimePromo'].astype(float)
            data['NotInMatrix'] = data['NotInMatrix'].astype(int)
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'}) 

        data.loc[data['SalePriceBeforePromo'] < data['SalePriceTimePromo'], 'SalePriceBeforePromo'] = data.loc[data['SalePriceBeforePromo'] < data['SalePriceTimePromo'], 'SalePriceTimePromo']

        #выделяем сезонные и не сезонные товары
        data = data.merge(itemdict[['ITEM', 'SUBCLASS']], how='left', left_on='Item', right_on='ITEM')
        data_lgbm = data[~data['SUBCLASS'].isin(season_items)]
        data_promo = data[data['SUBCLASS'].isin(season_items)]
        
        access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1NTk0ODcxNjIsIm5iZiI6MTU1OTQ4NzE2MiwianRpIjoiMzI4MmNiODUtYTUxZS00YTRhLTg1OGQtYTNkN2VkODY2ZTBmIiwiaWRlbnRpdHkiOiJzcGFyIiwiZnJlc2giOmZhbHNlLCJ0eXBlIjoiYWNjZXNzIn0.n0YvNuFeHuyanyjZdEJQ9zHox6P2F9EETetIA1Eylgw'
        headers = {'content-type': 'application/json', "Authorization": "Bearer {}".format(access_token)}
        
        #формируем запросы
        rs = []
        for u in urls_lgbm:
            if u == f'{PLAN_HOST_IP}/promo_lightgbm':
                to_dict = data_lgbm[["Item", "Analog", "ObjCode", "DateBegin", "DateEnd", "DateFinal", "PromoTypeCode", "SalePriceBeforePromo", "SalePriceTimePromo", "NotInMatrix"]].to_dict(orient = 'split')
                to_dict.pop('index');
                rs.append(grequests.post(u, data=json.dumps(to_dict, default = myconverter), headers=headers))
            elif u == f'{PLAN_HOST_IP}/promo':
                to_dict = data_promo[["Item", "Analog", "ObjCode", "DateBegin", "DateEnd", "DateFinal", "PromoTypeCode", "SalePriceBeforePromo", "SalePriceTimePromo", "NotInMatrix"]].to_dict(orient = 'split')
                to_dict.pop('index');
                rs.append(grequests.post(u, data=json.dumps(to_dict, default = myconverter), headers=headers))
            elif u == f'{PLAN_HOST_IP}/nopromo':
                to_dict = data[["Item", "Analog", "ObjCode", "DateBegin", "DateEnd", "DateFinal", "PromoTypeCode", "SalePriceBeforePromo", "SalePriceTimePromo", "NotInMatrix"]].to_dict(orient = 'split')
                to_dict.pop('index')
                rs.append(grequests.post(u, data=json.dumps(to_dict, default = myconverter), headers=headers))
        
        try:            
            df = pd.DataFrame()
            for r in grequests.map(rs):
                to_concat = pd.DataFrame(data = json.loads(r.text)['data'], columns = json.loads(r.text)['columns'])
                if len(to_concat) == len(data_lgbm):
                    data_lgbm['OnPromo'] = pd.DataFrame(data = json.loads(r.text)['data'], columns = json.loads(r.text)['columns'])['Lightgbm'].values
                elif len(to_concat) == len(data_promo):
                    data_promo['OnPromo'] = pd.DataFrame(data = json.loads(r.text)['data'], columns = json.loads(r.text)['columns'])['OnPromo'].values
                elif len(to_concat) == len(data):
                    data['BeforePromo'] = pd.DataFrame(data = json.loads(r.text)['data'], columns = json.loads(r.text)['columns'])['BeforePromo'].values
                    data['AfterPromo'] = pd.DataFrame(data = json.loads(r.text)['data'], columns = json.loads(r.text)['columns'])['AfterPromo'].values

            #собираем ответы
            df = data[['ObjCode', 'Item', 'BeforePromo', 'AfterPromo']]
            df.loc[data_promo.index, 'OnPromo'] = data_promo['OnPromo']
            df.loc[data_lgbm.index, 'OnPromo'] = data_lgbm['OnPromo']

            to_dict = df[['ObjCode', 'Item', 'BeforePromo', 'OnPromo', 'AfterPromo']].to_dict(orient = 'split')
            to_dict.pop('index')
            to_dict.update({'status':'success'})
            return jsonify(to_dict)

        except Exception as e:
            print(e)
            return jsonify({'status':'No access to remote server'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})

    
@app.route("/catalog", methods=['POST'])
@jwt_required
def catalog():    
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['EntityId', 'Item', 'ReplacementCoeff', 'Type']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
#         if len(data[data.duplicated(subset = ['DateFact', 'Item'])]) > 0:
#             return jsonify({'status':'duplicate data by \'DateFact\' and \'Item\' fields'})
        
        if sum(data[data['Type'] == 1].duplicated(subset = ['EntityId'])) > 0:
            return jsonify({'status':'EntityId must be unique if Type = 1'})
        
        try:
            data['EntityId'] = data['EntityId'].astype(int)
            data['Item'] = data['Item'].astype(int)
            data['ReplacementCoeff'] = data['ReplacementCoeff'].astype(float)
            data['Type'] = data['Type'].astype(int)
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})
            
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text(container_name, 
                                                 r'catalog/{}/catalog.csv'.format(df_dict['ObjCode']), output)
        return jsonify({'status':'success'})    
    
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})
    
    
@app.route("/consumption", methods=['POST'])
@jwt_required
def consumption():    
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['DateFact', 'Item', 'Qnty', 'Type', 'CodeOperation']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
        if len(data[data.duplicated(subset = ['DateFact', 'Item', 'Type', 'CodeOperation'])]) > 0:
            return jsonify({'status':'duplicated data by \'DateFact\', \'Item\', \'Type\' and \'CodeOperation\' fields'})
        
        try:
            data['DateFact'] = pd.to_datetime(data['DateFact'])
            data['Item'] = data['Item'].astype(int)
            data['Qnty'] = data['Qnty'].astype(float)
            data['Type'] = data['Type'].astype(int)
            data['CodeOperation'] = data['CodeOperation'].astype(int)
            max_date = data['DateFact'].max().date()
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type'})
            
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text(container_name, 
                                                 r'consumption/{}/{}.csv'.format(df_dict['ObjCode'],
                                                         max_date), output)
        return jsonify({'status':'success'})
    
    
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


## New methods

@app.route("/customerchecks", methods=['POST'])
@jwt_required
def customerchecks(): 
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['ClosingDate', 'UID', 'Item', 'Qnty', 'PriceBace', 'Discount', 'SumItem', 'SumTotal', 'CardID', 'CheckType']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
        if len(data[data.duplicated(subset = ['ClosingDate', 'Item', 'CheckType'])]) > 0:
            return jsonify({'status':'duplicated data by \'ClosingDate\', \'Item\'  and \'CheckType\' fields'})
        
        try:
            data['ClosingDate'] = pd.to_datetime(data['ClosingDate'])
            data['Item'] = data['Item'].astype(int)
            data['Qnty'] = data['Qnty'].astype(float)
            data['PriceBace'] = data['PriceBace'].astype(float)
            data['Discount'] = data['Discount'].astype(float)
            data['SumItem'] = data['SumItem'].astype(float)
            data['SumTotal'] = data['SumTotal'].astype(float)
            data['CardID'] = data['CardID'].astype(str)
            data['CheckType'] = data['CheckType'].astype(str)
            max_date = data['ClosingDate'].max().date()
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})
            
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                 r'customerchecks/{}/{}.csv'.format(df_dict['store_id'],
                                                         max_date), output)
        return jsonify({'status':'success'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/onlinechecks", methods=['POST']) 
@jwt_required
def onlinechecks(): 
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['ClosingDate', 'UID', 'Item', 'Qnty', 'PriceBace', 'Discount', 'SumItem', 'SumTotal', 'CardID', 'CheckType']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
        if len(data[data.duplicated(subset = ['ClosingDate', 'Item', 'CheckType'])]) > 0:
            return jsonify({'status':'duplicated data by \'ClosingDate\', \'Item\'  and \'CheckType\' fields'})
        
        try:
            data['ClosingDate'] = pd.to_datetime(data['ClosingDate'])
            data['Item'] = data['Item'].astype(int)
            data['Qnty'] = data['Qnty'].astype(float)
            data['PriceBace'] = data['PriceBace'].astype(float)
            data['Discount'] = data['Discount'].astype(float)
            data['SumItem'] = data['SumItem'].astype(float)
            data['SumTotal'] = data['SumTotal'].astype(float)
            data['CardID'] = data['CardID'].astype(str)
            data['CheckType'] = data['CheckType'].astype(str)
            max_date = data['ClosingDate'].max().date()
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})
            
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                 r'onlinechecks/{}/{}.csv'.format(df_dict['store_id'],
                                                         max_date), output)
        return jsonify({'status':'success'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/deliveries", methods=['POST'])
@jwt_required
def deliveries():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                        account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['ContractCode', 'DeliveryDate', 'DIVISION', 'DaysToDelivery']))
        if not columns_cheking:
                return jsonify({'status':'invalid columns or data format'})

        if len(data[data.duplicated()]) > 0:
                return jsonify({'status':'duplicated data'})
        
        try:
            data['DeliveryDate'] = pd.to_datetime(data['DeliveryDate'])
            data['DaysToDelivery'] = data['DaysToDelivery'].astype(int)
            data['DIVISION'] = data['DIVISION'].astype(int)
        
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})

        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                    r'deliveries/{}/{}/{}.csv'.format(df_dict['store_id'], df_dict['action'], datetime.now()), output)
        return jsonify({'status':'success'})

    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})



@app.route("/provider", methods=['POST'])
@jwt_required
def provider():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                        account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        #columns_cheking = (set(data.columns) == set(['Item', 'DIVISION', 'GROUP_NO', 'DEPT', 'CLASS', 'SUBCLASS', 'ContractCode', "ContragentCode", 'Quantum',
        #                                                'MinSafetyStock', 'PeriodSafetyStock', 'DateBegin', 'DateEnd', 'SafetyStock', 'Unit']))
        
        #if not columns_cheking:
        #        return jsonify({'status':'invalid columns or data format'})

        if len(data[data.duplicated(subset = ['Item'])]) > 0:
                return jsonify({'status':'duplicated data by \'Item\' field'})
        
        try:
            data = data.astype({'Item': 'int', 
            'DIVISION': 'int', 
            'GROUP_NO': 'int',
            'DEPT': 'int',
            'CLASS': 'int',
            'SUBCLASS': 'int', 
            'ContractCode': 'str',
            'Quantum':'str',
            'MinSafetyStock':'float', 
            'PeriodSafetyStock':'float', 
            'DateBegin': 'datetime64', 
            'DateEnd': 'datetime64', 
            'SafetyStock':'float', 
            'Unit':'str'})
        
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})

        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                    r'provider/{}/{}/{}.csv'.format(df_dict['store_id'], df_dict['action'], datetime.now()), output)
        return jsonify({'status':'success'})

    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/inventory", methods=['POST'])
@jwt_required
def inventory():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                        account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['Date', 'Item', 'Type', 'Qnty', 'SumTotal']))
        if not columns_cheking:
                return jsonify({'status':'invalid columns or data format'})

        if len(data[data.duplicated(subset = ['Date', 'Item', 'Type'])]) > 0:
                return jsonify({'status':'duplicated data by \'Date\', \'Item\'  and \'Type\' fields'})
        
        try:
            data = data.astype({'Date': 'datetime64', 
            'Item': 'int', 
            'Type': 'int',
            'Qnty':'float', 
            'SumTotal':'float'})
            max_date = data['Date'].max().date()
        
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})

        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                    r'inventory/{}/{}.csv'.format(df_dict['store_id'], max_date), output)
        return jsonify({'status':'success'})

    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})

@app.route("/clients", methods=['POST'])
@jwt_required
def clients():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                        account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['ClientID', 'CardID']))
        if not columns_cheking:
                return jsonify({'status':'invalid columns or data format'})

        if len(data[data.duplicated(subset = ['ClientID', 'CardID'])]) > 0:
                return jsonify({'status':'duplicated data by \'ClientID\'  and \'CardID\' fields'})
        
        try:
            data = data.astype({'CardID': 'str', 
            'ClientID': 'str'})
        
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})

        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                    r'clients/{}.csv'.format(datetime.now()), output)
        return jsonify({'status':'success'})

    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/cooking_map", methods=['POST'])
@jwt_required
def cooking_map():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                        account_key=key)
        df_dict = request.get_json()

        for cooking_map in df_dict['cooking_maps']:
            data = pd.DataFrame(data = cooking_map['data'], columns = cooking_map['columns'])
            #checking columns names
            columns_cheking = (set(data.columns) == set(['Item', 'Qnty', 'IngridientID', 'Brutto']))
            if not columns_cheking:
                    return jsonify({'status':'invalid columns or data format'})

            if len(data[data.duplicated(subset = ['Item', 'IngridientID'])]) > 0:
                    return jsonify({'status':'duplicated data by \'Item\'  and \'IngridientID\' fields'})
            
            try:
                data = data.astype({'Item': 'int', 
                'Qnty': 'float',
                'IngridientID':'int',
                'Brutto':'float'})
            
            except Exception as e:
                print(e)
                return jsonify({'status':'invalid columns type', 'description':str(e)})

            data['activity'] = cooking_map['activity']
            output = data.to_csv(index = False)
            block_blob_service.create_blob_from_text('checks', 
                                                        r'cookingmap/{}/{}.csv'.format(cooking_map['store_id'], cooking_map['RecipeID']), output)
        return jsonify({'status':'success'})

    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/cutting_map", methods=['POST'])
@jwt_required
def cutting_map():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                        account_key=key)
        df_dict = request.get_json()

        for cutting_map in df_dict['cutting_maps']:
            data = pd.DataFrame(data = cutting_map['data'], columns = cutting_map['columns'])
            #checking columns names
            columns_cheking = (set(data.columns) == set(['RawID', 'Coefficient', 'Item']))
            if not columns_cheking:
                    return jsonify({'status':'invalid columns or data format'})

            if len(data[data.duplicated(subset = ['Item', 'RawID'])]) > 0:
                    return jsonify({'status':'duplicated data by \'Item\'  and \'RawID\' fields'})
            
            try:
                data = data.astype({'RawID': 'str', 
                'Coefficient': 'float',
                'Item':'int'})
            
            except Exception as e:
                print(e)
                return jsonify({'status':'invalid columns type', 'description':str(e)})

            data['activity'] = cutting_map['activity']
            output = data.to_csv(index = False)
            block_blob_service.create_blob_from_text('checks', 
                                                        r'cuttingmaps/{}/{}.csv'.format(cutting_map['store_id'], cutting_map['RecipeID']), output)
        return jsonify({'status':'success'})

    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/complectation", methods=['POST']) 
@jwt_required
def complectation(): 
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['Date', 'Item', 'IngredientID', 'Coeff', 'Qnty']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
        if len(data[data.duplicated(subset = ['Item', 'IngredientID'])]) > 0:
            return jsonify({'status':'duplicated data by \'Item\'  and \'IngredientID\' fields'})
        
        try:
            data = data.astype({'Date':'datetime64', 
                                'Item': 'int', 
                                'IngredientID': 'int',
                                'Coeff':'float',
                                'Qnty':'float'})
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})

        max_date =  data['Date'].max().date()
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                 r'complectation/{}/{}.csv'.format(df_dict['store_id'],
                                                         max_date), output)
        return jsonify({'status':'success'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/orders", methods=['POST']) 
@jwt_required
def orders(): 
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['DatePlan', 'DateFinal', 'Item', 'Type', 'Qnty', 'Docid']))
        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
        if len(data[data.duplicated(subset = ['DatePlan', 'Item', 'Type', 'Docid'])]) > 0:
            return jsonify({'status':'duplicated data by \'DatePlan\', \'Item\', \'Type\' and  \'Docid\' fields'})
        
        try:
            data = data.astype({'DatePlan':'datetime64', 
                                'DateFinal':'datetime64',
                                'Item': 'int', 
                                'Type': 'int',
                                'Qnty':'float',
                                'Docid':'str'})
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})

        max_date =  str(datetime.now().date())
        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                 r'orders/{}/{}.csv'.format(df_dict['store_id'],
                                                         max_date), output)
        return jsonify({'status':'success'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})



@app.route("/loymax", methods=['POST']) 
@jwt_required
def loymax(): 
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()

        if not df_dict['GeneralInfo']['DeletionMark']:
            #Преобразование в байтовую строку
            output = json.dumps(df_dict).encode('utf-8')
            #Преобразование в поток
            output = io.BytesIO(output)
            block_blob_service.create_blob_from_stream('checks', 
                                            r'loymax/{}.json'.format(df_dict['GeneralInfo']['PWCguid']), output)

        else:
            try:
                block_blob_service.delete_blob('checks', r'loymax/{}.json'.format(df_dict['GeneralInfo']['PWCguid']))
            except Exception as e:
                return jsonify({'status': 'The specified discount guid does not exist'})
      
        return jsonify({'status':'success'})
        
    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/planograms", methods=['POST']) 
@jwt_required
def planograms(): 
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        data = pd.DataFrame(data = df_dict['data'], columns = df_dict['columns'])
        #checking columns names
        columns_cheking = (set(data.columns) == set(['WarehouseID', 'CodeTO', 'NameTO', 'EI', 'F',
        'H', 'D', 'Item', 'ItemName', 'ItemWidth', 'ItemHeight', 'ItemDepth', 'ShelfID', 'ShelfWidth', 'ShelfDepth']))

        if not columns_cheking:
            return jsonify({'status':'invalid columns or data format'})
        
        try:
            data = data.astype({
            'WarehouseID': 'int', 
            'CodeTO': 'int',
            'NameTO': 'str',
            'EI': 'str',
            'F': 'int',
            'H': 'int', 
            'D': 'int',
            'Item':'int',
            'ItemName':'str', 
            'ItemWidth':'float', 
            'ItemHeight': 'float', 
            'ItemDepth': 'float', 
            'ShelfID':'int', 
            'ShelfWidth':'float',
            'ShelfDepth':'float'})
        
        except Exception as e:
            print(e)
            return jsonify({'status':'invalid columns type', 'description':str(e)})

        output = data.to_csv(index = False)
        block_blob_service.create_blob_from_text('checks', 
                                                    r'planograms/{}/{}/{}.csv'.format(df_dict['ObjCode'], df_dict['action'], datetime.now()), output)
        return jsonify({'status':'success'})

    except Exception as e:
        return jsonify({'status': 'error', 'description':str(e)})


@app.route("/safetystock", methods=['POST'])
@jwt_required
def safetystock():
    try:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        df_dict = request.get_json()
        store_id = df_dict['store_id']
        try:
            with io.BytesIO() as input_blob:
                block_blob_service.get_blob_to_stream(container_name=container_name,
                                                  blob_name = r'safetystock/{}/safetystock.csv'.format(store_id),
                                                           stream = input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob)

            df_dict = df.to_dict(orient='split')
            df_dict.pop('index');
            df_dict.update({'store_id':store_id})
            df_dict1 = OrderedDict(store_id = df_dict['store_id'],
                                   columns = df_dict['columns'], data = df_dict['data'])

            return json.dumps(df_dict1)
        except Exception as e:
            return json.dumps({'status': 'error', 'description':str(e)})

    except:
        return json.dumps({'status':'something wrong, check json stucture'})


@app.route("/ss", methods=['POST'])
@jwt_required
def return_ss():
    """Запрос СЗ (новая версия)"""
    try:
        # parse store_id
        df_dict = request.get_json()
        store_id = df_dict['store_id']
        # init azure blob connection
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                              account_key=key)
        # checking that for the received store number there are safety stocks
        try:
            # load safety stocks
            with io.BytesIO() as input_blob:
                block_blob_service.get_blob_to_stream(container_name='checks',
                                                      blob_name=f'ss/{store_id}/data.csv',
                                                      stream=input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob)

                df_dict = df.to_dict(orient='split')
                df_dict.pop('index')
                df_dict.update({'store_id': store_id})
                df_dict1 = OrderedDict(store_id=df_dict['store_id'],
                                       columns=df_dict['columns'],
                                       data=df_dict['data'])

                return json.dumps(df_dict1)

        except Exception as e:
            return json.dumps({'status': 'load data error', 'description': str(e)})

    except Exception as e:
        return json.dumps({'status': 'error', 'description': str(e)})


@app.route("/pss", methods=['POST'])
@jwt_required
def pss():
    """Запрос ПСС (новая версия)"""
    try:
        # parse store_id
        df_dict = request.get_json()
        store_id = df_dict['store_id']
        # init azure blob connection
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                              account_key=key)
        # checking that for the received store number there are safety stocks
        try:
            # load safety stocks
            with io.BytesIO() as input_blob:
                block_blob_service.get_blob_to_stream(container_name='checks',
                                                      blob_name=f'pss/{store_id}/data.csv',
                                                      stream=input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob)

                df_dict = df.to_dict(orient='split')
                df_dict.pop('index')
                df_dict.update({'store_id': store_id})
                df_dict1 = OrderedDict(store_id=df_dict['store_id'],
                                       columns=df_dict['columns'],
                                       data=df_dict['data'])

                return json.dumps(df_dict1)

        except Exception as e:
            return json.dumps({'status': 'load data error', 'description': str(e)})

    except Exception as e:
        return json.dumps({'status': 'error', 'description': str(e)})