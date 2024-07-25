import streamlit as st 
from main import load_lottiefile 
from streamlit_lottie import st_lottie_spinner
import pandas as pd
from main import load_data_graph,run_queries,setup_tg_connection,setup_consumer,login,logout
from geopy.geocoders import Nominatim 
import requests
import time

st.set_page_config(
    page_title="Real-Time Data Consuming and Visualization",
    page_icon="✅",
    layout="wide",
)


lottie_file = "Animation-1716966760564.json"
lottie_animation = load_lottiefile(lottie_file)


try:
    conn=setup_tg_connection()
    Consumer=setup_consumer()
except Exception as e:
    st.write(f"Error occured {e}")

    
with st.sidebar:
    st.image("NVlogo.png",width=150)
    st.header("Credit Card Fraud")
    
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if st.session_state.logged_in:
    if st.sidebar.button("Log out"):
        logout()
else:
    if st.sidebar.button("Log in"):
        login()

if not st.session_state.logged_in:
    st.title("Please log in to access the application.")
    st.stop()
    
st.title("Real-Time Data Consuming and Visualization")

geolocator = Nominatim(user_agent="my_app")
def get_address(lat,lon):
    location = geolocator.reverse((lat+","+lon))
    if location is not None:
        return location.address
    else:
        return None

consume=st.button("Start Consuming Data!")

map_placeholder = st.empty()
transactions_placeholder = st.empty()
load_data_placeholder=st.empty()
placeholder_generate_features=st.empty()

load = -1
features=-1
c=0

if consume:  
    try: 
        for val in Consumer:
            data=pd.DataFrame(val.value)
            if not data.empty:
                
                with transactions_placeholder.container():
                    local_placeholder1=st.empty()
                    local_placeholder1.markdown(
                        "<h2 style='font-size:24px;'>1: Incoming Data Statistics</h2>", 
                        unsafe_allow_html=True
                    )
                    st.write("")
                    placeholder_data = st.empty()
                    placeholder_data.text("Incoming Transaction Details")
                    st.table(data[['cc_num', 'trans_num', 'amt', 'merchant', 'transaction_datetime']])
            
                    placeholder_address=st.empty()
                    placeholder_address.text("Transaction Initiated from Location")
                    data_1=data[["merch_lat","merch_long"]]
                    data_1["Address"]=data_1.apply(lambda x: get_address(str(x.merch_lat),str(x.merch_long)),axis=1)
                    data_1.columns=["Latitude","Longitude","Address"]
                    st.table(data_1)
                    
                
                with load_data_placeholder.container():
                    local_placeholder2=st.empty()
                    local_placeholder2.markdown(
                        "<h2 style='font-size:24px;'>2: Data Loading on Graph</h2>", 
                        unsafe_allow_html=True
                    )
                        
                    with st_lottie_spinner(lottie_animation, height=200, key=f"loading_animation_x+{c}"):
                        placeholder_loading = st.empty()
                        placeholder_loading.text("Loading the data, please wait...")
                        start_time=time.time()
                        load=load_data_graph(conn,data)
                        end_time=time.time()
                        placeholder_loading.text(f"Data Loaded on Graph in {int(end_time-start_time)} second")
                        st.table(data)
                    
                        
                if load==1:     
                    with placeholder_generate_features.container():
                        local_placeholder3=st.empty()
                        local_placeholder3.markdown(
                        "<h2 style='font-size:24px;'>3: Generate Graph Features</h2>", 
                        unsafe_allow_html=True)
                        
                        with st_lottie_spinner(lottie_animation, height=200, key=f"loading_animation_y+{c}"):
                            placeholder_features = st.empty()
                            placeholder_features.text("Generating Features, please wait...")
                            start_time=time.time()
                            features=run_queries(conn)
                            end_time=time.time()
                            
                            if features==1:
                                placeholder_features.text(f"Feature Generation completed in {int(end_time-start_time)} second")
                                placeholder_fename=st.empty()
                                with placeholder_fename.container():
                                    placeholder_variable_title=st.empty()
                                    placeholder_variable_title.text("Generated Features")
                                    st.table(pd.read_csv("features_details.csv"))
                        
                        if features==1:
                            prediction_placeholder=st.empty()
                            try:
                                response = requests.post("http://172.16.20.71:8000/api/predictor/predict")
                                
                                with prediction_placeholder.container():
                                    local_placeholder4=st.empty()
                                    local_placeholder4.markdown("<h2 style='font-size:24px;'>4: Prediction on New Transactions</h2>", 
                                                     unsafe_allow_html=True)
                                    
                                    placeholder_model=st.empty()
                                    placeholder_model.text("Model Prediction")
                                    st.table(pd.DataFrame(response.json()))
                                    
                            except Exception as e:
                                st.error(f"An error occurred: {e}")
                c=c+1
        
    except Exception as e:
        st.write(f"Error message {e}")













    

    

