import streamlit as st
import pandas as pd
import pyTigerGraph as tg
from json import loads
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from kafka import KafkaConsumer
import json

geolocator = Nominatim(user_agent="geoapiExercises")
    
def run_queries(conn):
    try:
        conn.runInstalledQuery("update_min_max_spent")
        conn.runInstalledQuery("distance_time_from_last_transaction_for_newtransaction")
        conn.runInstalledQuery("InsertBatchNo",params={"BatchNo":-1})
        conn.runInstalledQuery("tg_degree_cent",params={"v_type_set":["Merchant","Merchant_Category","CreditCard"],"e_type_set":["towards","of_category","belongs_to"],"reverse_e_type_set":["towards","of_category","belongs_to"],"result_attribute":"ML_degCen"})
        conn.runInstalledQuery("fast_rp",params={"v_type_set":["CreditCard","Transaction","Merchant","Merchant_Category"],"e_type_set":["towards","of_category","belongs_to"],"output_v_type_set":["Transaction"],"iteration_weights":"1,2,4","embedding_dimension":4,"default_length":4,
                                                 "result_attribute":"ML_embedding","component_attribute":"ML_batch_no","batch_number":-1})
        conn.runInstalledQuery("InsertBatchNo",params={"BatchNo":0})
        return 1
    except Exception as e:
        st.write(str(e))
        return -1
    
def load_data_graph(conn,df):
    try:
        conn.upsertVertexDataFrame(df=df,vertexType="Location",v_id="merch_loc_id",attributes={'Loc_id': "merch_loc_id",'Lat'   : "merch_lat",
                'Lon'  : "merch_long"})
        
        conn.upsertVertexDataFrame(
            df=df,vertexType="Transaction",v_id="trans_num",attributes={'Transaction_id': "trans_num",'Transaction_Datetime'   : "transaction_datetime",
                'Amount'   : "amt",'ML_prediction' : "is_fraud","ML_Production_Data":"ML_Production_Data"})
        
        conn.upsertEdgeDataFrame(df=df,sourceVertexType="Transaction",edgeType = "located",targetVertexType = "Location",from_id = "trans_num",to_id = "merch_loc_id",attributes = {})
        
        conn.upsertEdgeDataFrame(df=df,sourceVertexType="Transaction",edgeType = "belongs_to",targetVertexType = "CreditCard",from_id = "trans_num",
            to_id = "cc_num",attributes = {})
        
        conn.upsertEdgeDataFrame(df=df,sourceVertexType="Transaction",edgeType = "of_category",targetVertexType = "Merchant_Category",from_id = "trans_num",
            to_id = "category",attributes = {})
        
        conn.upsertEdgeDataFrame(df=df,sourceVertexType="Transaction",edgeType = "towards",targetVertexType = "Merchant",from_id = "trans_num",
            to_id = "merchant",attributes = {})
        return 1
    except Exception as e:
        st.write(str(e))
        return -1
    
    
def load_lottiefile(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)
    
    
@st.cache_resource
def setup_tg_connection():
    hostName = "https://172.16.20.71/"
    graphName = "CreditCardFraud"
    secret ="23ppgurt24vcf0a84b5dfbdk88gm701l"
    conn = tg.TigerGraphConnection(host=hostName,graphname=graphName, gsqlSecret=secret,tgCloud=False)
    conn = tg.TigerGraphConnection(host=hostName,graphname=graphName, gsqlSecret=secret,tgCloud=False,apiToken=conn.getToken(secret)[0])
    return conn

@st.cache_resource
def setup_consumer():
    consumer = KafkaConsumer(
        "creditcardfraud",
        bootstrap_servers=["172.16.20.71:9092"],
        value_deserializer=lambda x: loads(x.decode('utf-8'))
        )
    
    return consumer

def login():
    st.session_state.logged_in = True
    
def logout():
    st.session_state.logged_in = False
