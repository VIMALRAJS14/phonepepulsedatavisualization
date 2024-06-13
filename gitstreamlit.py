import streamlit as st
from streamlit_option_menu import option_menu
import phonepepulse
import plotly.express as px 
st.title("PHONEPE PULSE DATA")
with st.sidebar:
    option=option_menu("select option",['TRANSACTION DATA','USER DATA','VISUALIZATION'])
if option=='TRANSACTION DATA':
    SELECT=st.radio("SELECT OPTION TO VIEW",('TRANSACTIONDATA','TRANSACTIONMAP','TOPTRANSACTIONS'),index=0)
    if SELECT=='TRANSACTIONDATA':
        year=st.selectbox('select year',list(range(2018,2024)))
        quarter=st.selectbox('select quarter',['1','2','3','4'])
        output1=phonepepulse.transactionquery(year,quarter)
        output2=phonepepulse.totaltransaction(year,quarter)
        st.dataframe(output2)
        st.dataframe(output1)
    if SELECT=='TRANSACTIONMAP':
        year=st.selectbox('select year',list(range(2018,2024)))
        quarter=st.selectbox('select quarter',['1','2','3','4'])
        output1=phonepepulse.transactionmap(int(year),int(quarter))
        st.plotly_chart(output1)
    if SELECT=='TOPTRANSACTIONS':
        year=st.selectbox('select year',list(range(2018,2024)))
        quarter=st.selectbox('select quarter',['1','2','3','4'])
        option=st.radio("SELECT OPTION TO VIEW",('STATE','DISTRICT','PINCODES'))   
        if option=='STATE':
            output=phonepepulse.toptransactionstates(year,quarter)
            st.dataframe(output)
        if option=='DISTRICT':
            output=phonepepulse.toptransactiondistricts(year,quarter)
            st.dataframe(output)
        if option=='PINCODES':
            output=phonepepulse.toptransactionpincodes(year,quarter)
            st.dataframe(output)
if option=='USER DATA':
    SELECT=st.radio("SELECT OPTION TO VIEW",('USERDATA','USERMAP','TOPUSERS'),index=0)
    if SELECT=='USERDATA':
        year=st.selectbox('select year',list(range(2018,2024)))
        quarter=st.selectbox('select quarter',['1','2','3','4'])
        output=phonepepulse.userdata(year,quarter)
        st.dataframe(output)
    if SELECT=='USERMAP':
        year=st.selectbox('select year',list(range(2018,2024)))
        quarter=st.selectbox('select quarter',['1','2','3','4'])
        output1=phonepepulse.usermap(int(year),int(quarter))
        st.plotly_chart(output1)
    if SELECT=='TOPUSERS':
        year=st.selectbox('select year',list(range(2018,2024)))
        quarter=st.selectbox('select quarter',['1','2','3','4'])
        option=st.radio("SELECT OPTION TO VIEW",('STATE','DISTRICT','PINCODES')) 
        if option=='STATE':
            output=phonepepulse.topuserstates(year,quarter)
            st.dataframe(output)
        if option=='DISTRICT':
            output=phonepepulse.topuserdistricts(year,quarter)
            st.dataframe(output)
        if option=='PINCODES':
            output=phonepepulse.topuserpincodes(year,quarter)
            st.dataframe(output)
if option=='VISUALIZATION':
    SELECT=st.selectbox("select visualization",['VISUALIZATION1','VISUALIZATION2','VISUALIZATION3','VISUALIZATION4','VISUALIZATION5','VISUALIZATION6','VISUALIZATION7','VISUALIZATION8','VISUALIZATION9'])
    if SELECT=='VISUALIZATION1':
        output=phonepepulse.visualization1()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION2':
        output=phonepepulse.visualization2()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION3':
        output=phonepepulse.visualization3()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION4':
        output=phonepepulse.visualization4()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION5':
        output=phonepepulse.visualization5()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION6':
        output=phonepepulse.visualization6()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION7':
        output=phonepepulse.visualization7()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION8':
        output=phonepepulse.visualization8()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION9':
        output=phonepepulse.visualization9()
        st.plotly_chart(output)
    if SELECT=='VISUALIZATION10':
        output=phonepepulse.visualization10()
        st.plotly_chart(output)
          