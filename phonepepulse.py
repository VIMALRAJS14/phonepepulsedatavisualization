import os
import json
import pandas as pd
import mysql.connector
import plotly.express as px
mydb=mysql.connector.connect(host="localhost",user="root",password="")
mycursor=mydb.cursor(buffered=True)
DONE='done'
from sqlalchemy import create_engine
engine=create_engine(f"mysql+mysqlconnector://root@localhost:3306/phonepepulse")
statenames={'chhattisgarh':'Chhattisgarh','uttarakhand':'Uttarakhand','delhi':'Delhi','jammu-&-kashmir':'Jammu & Kashmir','tripura':'Tripura','goa':'Goa','assam':'Assam','mizoram':'Mizoram','maharashtra':'Maharashtra','ladakh':'Ladakh','himachal-pradesh':'Himachal Pradesh','west-bengal':'West Bengal','bihar':'Bihar','tamil-nadu':'Tamil Nadu','arunachal-pradesh':'Arunachal Pradesh','rajasthan':'Rajasthan','sikkim':'Sikkim','gujarat':'Gujarat','nagaland':'Nagaland','puducherry':'Puducherry','lakshadweep':'Lakshadweep','andaman-&-nicobar-islands':'Andaman & Nicobar','andhra-pradesh':'Andhra Pradesh','odisha':'Odisha','telangana':'Telangana','dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar Haveli and Daman and Diu','madhya-pradesh':'Madhya Pradesh','kerala':'Kerala','meghalaya':'Meghalaya','manipur':'Manipur','punjab':'Punjab','jharkhand':'Jharkhand','karnataka':'Karnataka','uttar-pradesh':'Uttar Pradesh','haryana':'Haryana','chandigarh':'Chandigarh'}
def aggregatetransaction():
    state=[]
    year=[]
    quarter=[]
    transaction_type=[]
    transaction_count=[]
    transaction_amount=[]
    aggtran=r"C:\Users\VIMALRAJ S\Downloads\gitdata\pulse\data\aggregated\transaction\country\india\state/"
    aggstatelist=os.listdir(aggtran)
    for i in aggstatelist:
      aggstatepath=aggtran+i
      aggyearlist=os.listdir(aggstatepath)
      for j in aggyearlist:
        aggyearpath=aggstatepath+'/'+j
        aggjsonlist=os.listdir(aggyearpath)
        for k in aggjsonlist:
          aggjsonfile=aggyearpath+'/'+k
          data=open(aggjsonfile,'r')
          file=json.load(data)
          for z in file['data']['transactionData']:
            state.append(i)
            year.append(j)
            quarter.append(k.strip('.json'))
            transaction_type.append(z['name'])
            transaction_count.append(z['paymentInstruments'][0]['count'])
            transaction_amount.append(int(z['paymentInstruments'][0]['amount']))
    statealter=list(map(statenames.get,state))
    aggtransaction={'state':statealter,'year':year,'quarter':quarter,'type':transaction_type,'count':transaction_count,'amount':transaction_amount}
    aggtransactiond=pd.DataFrame(aggtransaction)
    mycursor.execute("create database if not exists phonepepulse")
    mycursor.execute("create table if not exists phonepepulse.aggregatetransaction (state VARCHAR(255),year YEAR,quarter INT,type TEXT,count BIGINT,amount BIGINT)")
    aggtransactiond.to_sql('aggregatetransaction',con=engine,if_exists='replace',index=False)
    return DONE
def aggregateuser():
    state=[]
    year=[]
    quarter=[]
    registereduser=[]
    appopens=[]
    brand=[]
    count=[]
    agguse=r"C:\Users\VIMALRAJ S\Downloads\gitdata\pulse\data\aggregated\user\country\india\state"
    aggstatelist=os.listdir(agguse)
    for i in aggstatelist:
        aggstatepath=agguse+'/'+i
        aggyearlist=os.listdir(aggstatepath)
        for j in aggyearlist:
            aggyearpath=aggstatepath+'/'+j
            aggjsonlist=os.listdir(aggyearpath)
            for k in aggjsonlist:
                aggjsonfile=aggyearpath+'/'+k
                data=open(aggjsonfile,'r')
                file=json.load(data)
                if file['data']['usersByDevice'] is not None:
                    for z in file['data']['usersByDevice']:
                      state.append(i)
                      year.append(j)
                      quarter.append(k.strip('.json'))
                      registereduser.append(file['data']['aggregated']['registeredUsers'])
                      appopens.append(file['data']['aggregated']['appOpens'])
                      brand.append(z['brand'])
                      count.append(z['count'])
                else:
                      state.append(i)
                      year.append(j)
                      quarter.append(k.strip('.json'))
                      registereduser.append(file['data']['aggregated']['registeredUsers'])
                      appopens.append(file['data']['aggregated']['appOpens'])
                      brand.append('0')
                      count.append('0')
    statealter=list(map(statenames.get,state))
    agguser={'state':statealter,'year':year,'quarter':quarter,'registereduser':registereduser,'appopens':appopens,'brand':brand,'count':count}
    agguserd=pd.DataFrame(agguser)
    mycursor.execute("create table if not exists phonepepulse.aggregateuser (state VARCHAR(255),year YEAR,quarter INT,registereduser BIGINT,appopens BIGINT,brand VARCHAR(255),count BIGINT)")
    agguserd.to_sql('aggregateuser',con=engine,if_exists='replace',index=False)
    return DONE
def maptransaction():
    state=[]
    year=[]
    quarter=[]
    districtname=[]
    count=[]
    amount=[]
    maptranpath=r"C:\Users\VIMALRAJ S\Downloads\gitdata\pulse\data\map\transaction\hover\country\india\state"
    mapstatelist=os.listdir(maptranpath)
    for i in mapstatelist:
        mapstatepath=maptranpath+'/'+i
        mapyearlist=os.listdir(mapstatepath)
        for j in mapyearlist:
            mapyearpath=mapstatepath+'/'+j
            mapjsonlist=os.listdir(mapyearpath)
            for k in mapjsonlist:
                mapjsonfile=mapyearpath+'/'+k
                data=open(mapjsonfile,'r')
                file=json.load(data)
                for z in file['data']['hoverDataList']:
                  state.append(i)
                  year.append(j)
                  quarter.append(k.strip('.json'))
                  districtname.append(z['name'])
                  count.append(z['metric'][0]['count'])
                  amount.append(int(z['metric'][0]['amount']))
    statealter=list(map(statenames.get,state))
    maptransaction={'state':statealter,'year':year,'quarter':quarter,'districtname':districtname,'count':count,'amount':amount}
    maptransactiond=pd.DataFrame(maptransaction)
    mycursor.execute("create table if not exists phonepepulse.maptransaction (state VARCHAR(255),year YEAR,quarter INT,districtname TEXT,count BIGINT,amount BIGINT)")
    maptransactiond.to_sql('maptransaction',con=engine,if_exists='replace',index=False)
    return DONE
def mapuser():
    state=[]
    year=[]
    quarter=[]
    districtname=[]
    registeredusers=[]
    appopens=[]
    mapuserpath=r"C:\Users\VIMALRAJ S\Downloads\gitdata\pulse\data\map\user\hover\country\india\state"
    mapstatelist=os.listdir(mapuserpath)
    for i in mapstatelist:
        mapstatepath=mapuserpath+'/'+i
        mapyearlist=os.listdir(mapstatepath)
        for j in mapyearlist:
            mapyearpath=mapstatepath+'/'+j
            mapjsonlist=os.listdir(mapyearpath)
            for k in mapjsonlist:
                mapjsonfile=mapyearpath+'/'+k
                data=open(mapjsonfile,'r')
                file=json.load(data)
                for z in file['data']['hoverData'].items():
                  state.append(i)
                  year.append(j)
                  quarter.append(k.strip('.json'))
                  districtname.append(z[0])
                  registeredusers.append(z[1]['registeredUsers'])
                  appopens.append(z[1]['appOpens'])
    statealter=list(map(statenames.get,state))
    mapuser={'state':statealter,'year':year,'quarter':quarter,'districtname':districtname,'registeredusers':registeredusers,'appopens':appopens}
    mapuserd=pd.DataFrame(mapuser)
    mycursor.execute("create table if not exists phonepepulse.mapuser (state VARCHAR(255),year YEAR,quarter INT,districtname TEXT,registeredusers BIGINT,appopens BIGINT)")
    mapuserd.to_sql('mapuser',con=engine,if_exists='replace',index=False)
    return DONE
def toptransaction():
    state=[]
    year=[]
    quarter=[]
    pincodes=[]
    count=[]
    amount=[]
    toppath=r"C:\Users\VIMALRAJ S\Downloads\gitdata\pulse\data\top\transaction\country\india\state"
    topstatelist=os.listdir(toppath)
    for i in topstatelist:
        topstatepath=toppath+'/'+i
        topyearlist=os.listdir(topstatepath)
        for j in topyearlist:
            topyearpath=topstatepath+'/'+j
            topjsonlist=os.listdir(topyearpath)
            for k in topjsonlist:
                topjsonfile=topyearpath+'/'+k
                data=open(topjsonfile,'r')
                file=json.load(data) 
                for z in file['data']['pincodes']:
                    state.append(i)
                    year.append(j)
                    quarter.append(k.strip('.json'))
                    pincodes.append(z['entityName'])
                    count.append(z['metric']['count'])
                    amount.append(int(z['metric']['amount']))
    statealter=list(map(statenames.get,state))
    toptransaction={'state':statealter,'year':year,'quarter':quarter,'pincodes':pincodes,'count':count,'amount':amount}
    toptransactiond=pd.DataFrame(toptransaction)
    mycursor.execute("create table if not exists phonepepulse.toptransaction (state VARCHAR(255),year YEAR,quarter INT,pincodes INT,count BIGINT,amount BIGINT)")
    toptransactiond.to_sql('toptransaction',con=engine,if_exists='replace',index=False)
    return DONE
def topuser():
    state=[]
    year=[]
    quarter=[]
    pincodes=[]
    registeredusers=[]
    topuserpath=r"C:\Users\VIMALRAJ S\Downloads\gitdata\pulse\data\top\user\country\india\state"
    topstatelist=os.listdir(topuserpath)
    for i in topstatelist:
        topstatepath=topuserpath+'/'+i
        topyearlist=os.listdir(topstatepath)
        for j in topyearlist:
            topyearpath=topstatepath+'/'+j
            topjsonlist=os.listdir(topyearpath)
            for k in topjsonlist:
                topjsonpath=topyearpath+'/'+k
                data=open(topjsonpath,'r')
                file=json.load(data)
                for z in file['data']['pincodes']:
                    state.append(i)
                    year.append(j)
                    quarter.append(k.strip('.json'))
                    pincodes.append(z['name'])
                    registeredusers.append(z['registeredUsers'])
    statealter=list(map(statenames.get,state))
    topuser={'state':statealter,'year':year,'quarter':quarter,'pincodes':pincodes,'registeredusers':registeredusers}
    topuserd=pd.DataFrame(topuser)
    mycursor.execute("create table if not exists phonepepulse.topuser (state VARCHAR(255),year YEAR,quarter INT,pincodes INT,registeredusers BIGINT)")
    topuserd.to_sql('topuser',con=engine,if_exists='replace',index=False)
    return DONE

def main():
    aggregatetransactions=aggregatetransaction()
    aggregateusers=aggregateuser()
    maptransactions=maptransaction()
    mapusers=mapuser()
    toptransactions=toptransaction()
    topusers=topuser()
    return aggregatetransactions,aggregateusers,maptransactions,mapusers,toptransactions,topusers
def transactionquery(year,quarter):
    query="select type,sum(amount) as TOTALAMOUNT from phonepepulse.aggregatetransaction where year=%s and quarter=%s group by type "
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def totaltransaction(year,quarter):
    query="select sum(count) AS TOTALTRANSACTIONS,sum(amount) AS TOTALAMOUNT,sum(amount)/sum(count) AS AVERAGETRANSACTION from phonepepulse.aggregatetransaction where year=%s and quarter=%s"
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def transactionmap(year,quarter):
    query='select state,CAST(sum(count) AS SIGNED) AS TOTALTRANSACTION,CAST(sum(amount) AS SIGNED) AS TOTALPAYMENTVALUE,sum(amount)/sum(count) AS AVERAGETRANSACTION from phonepepulse.aggregatetransaction where year=%s and quarter=%s group by state'
    mycursor.execute(query,(int(year),int(quarter)))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig = px.choropleth(
    output,
    geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
    featureidkey='properties.ST_NM',
    locations='state',
    color='TOTALTRANSACTION',
    hover_name='state',
    hover_data={'TOTALPAYMENTVALUE':True,'AVERAGETRANSACTION':True,'state':False},
    color_continuous_scale='Reds')
    fig.update_geos(fitbounds="locations", visible=False)
    return fig
def toptransactionstates(year,quarter):
    query='select state,CAST(amount as SIGNED) AS TOTALAMOUNT from phonepepulse.aggregatetransaction where year=%s and quarter=%s group by state order by amount  DESC LIMIT 10'
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def toptransactiondistricts(year,quarter):
    query='select districtname,CAST(amount as SIGNED) AS TOTALAMOUNT from phonepepulse.maptransaction where year=%s and quarter=%s group by districtname order by amount  DESC LIMIT 10'
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def toptransactionpincodes(year,quarter):
    query='select pincodes,CAST(amount as SIGNED) AS TOTALAMOUNT from phonepepulse.toptransaction where year=%s and quarter=%s group by pincodes order by amount  DESC LIMIT 10'
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def userdata(year,quarter):
    query='select sum(registereduser) AS TOTALREGISTEREDUSERS,sum(appopens) AS APPOPENS FROM (select registereduser,appopens from phonepepulse.aggregateuser where year=%s and quarter=%s group by state) AS statelist'
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def usermap(year,quarter):
    query='select state,CAST(registereduser as signed) AS registereduser,CAST(appopens as signed) AS appopens from phonepepulse.aggregateuser where year=%s and quarter=%s group by state'
    mycursor.execute(query,(int(year),int(quarter)))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig=px.choropleth(
        output,
        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
        locations='state',
        featureidkey='properties.ST_NM',
        color='registereduser',
        hover_data={'appopens':True},
        color_continuous_scale='Reds')
    fig.update_geos(fitbounds="locations", visible=False)
    return fig
def topuserstates(year,quarter):
    query='select state,registereduser from phonepepulse.aggregateuser where year=%s and quarter=%s group by state order by registereduser DESC LIMIT 10'
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def topuserdistricts(year,quarter):
    query='select districtname,registeredusers from phonepepulse.mapuser where year=%s and quarter=%s group by districtname order by registeredusers DESC LIMIT 10'
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def topuserpincodes(year,quarter):
    query='select pincodes,registeredusers from phonepepulse.topuser where year=%s and quarter=%s group by pincodes order by registeredusers DESC LIMIT 10'
    mycursor.execute(query,(year,quarter))
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    records=output.to_records(index=False)
    return records
def visualization1():
    query='select year,quarter,state,brand,count from phonepepulse.aggregateuser'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig = px.sunburst(output, path=['year', 'quarter','state','brand'], values='count')
    fig.update_layout(title='sunburst chart indicating different brand users')
    return fig
def visualization2():
    query='select year,quarter,CAST(sum(amount)/sum(count) as signed)As Averagetransaction from phonepepulse.aggregatetransaction group by year,quarter'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig=px.bar(output,y="Averagetransaction",x='year',color='quarter',barmode='group')
    fig.update_layout(title='average transaction in years')
    return fig
def visualization3():
    query='select brand,sum(count) AS COUNT from phonepepulse.aggregateuser group by brand'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig = px.pie(values=output['COUNT'], names=output['brand'])
    fig.update_layout(title='pie chart of mobile brands in phonepe')
    return fig
def visualization4():
    query='select state,sum(registereduser) as registereduser from phonepepulse.aggregateuser where year=2023 and quarter=4 group by state'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig=px.bar(output,x='state',y='registereduser')
    fig.update_layout(title='bar plot of registered users in each state')
    return fig
def visualization5():
    query='select type,sum(count) AS COUNT from phonepepulse.aggregatetransaction group by type'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig = px.pie(values=output['COUNT'],names=output['type'])
    fig.update_layout(title='pie chart for counts of each type')
    return fig
def visualization6():
    query='select subquery1.count,subquery2.appopens FROM (select year,sum(count) as count from phonepepulse.aggregatetransaction where year=2020 or year=2021 or year=2022 or year=2023 group by year) AS subquery1 JOIN (select year,sum(appopens) as appopens from phonepepulse.aggregateuser where year=2020 or year=2021 or year=2022 or year=2023 group by year) AS subquery2 ON subquery1.year=subquery2.year'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig = px.line(output,y='appopens',x='count')
    fig.update_layout(title='line plot foe count vs appopens')
    return fig
def visualization7():
    query='select year,type,sum(amount) as amount from phonepepulse.aggregatetransaction group by year,type'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig=px.bar(output,x='year',y='amount',color='type',barmode='group')
    fig.update_layout(title='bar plot for year wise each type amount')
    return fig
def visualization8():
    query='select year,CAST(sum(amount)as signed) as amount from phonepepulse.maptransaction group by year'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig=px.bar(output, x="year", y='amount')
    fig.update_layout(title='bar plot for yearwise transaction')
    return fig
def visualization9():
    query='select districtname,registeredusers from phonepepulse.mapuser where year=2023 and quarter=4 order by registeredusers desc limit 10'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig=px.scatter(output,x='districtname',y='registeredusers')
    fig.update_layout(title='scatter plot for top 10 registered users-district')
    return fig
def visualization10():
    query='select pincodes,year,registeredusers from phonepepulse.topuser where year=2023 and quarter=4 order by registeredusers desc limit 10'
    mycursor.execute(query)
    data=mycursor.fetchall()
    output=pd.DataFrame(data,columns=[i[0] for i in mycursor.description])
    fig=px.scatter(output,x='pincodes',y='registeredusers')
    fig.update_layout(title='scatter plot for top 10 registered users-pincode')
    return fig
main()
