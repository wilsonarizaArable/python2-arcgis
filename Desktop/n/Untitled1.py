import pandas as pd
import pymysql
import datetime 

from arcgis.features import SpatialDataFrame

from arcgis.gis import GIS
from IPython.display import display

from getpass import getpass

connection = pymysql.connect(
host='67.210.244.58',
port=3306,
user='multisol_root',
password='xude8i3e',
db='multisol_recorre2'
)


gis = GIS() 
item = gis.content.get("39f00564a0aa439a85174913be7e792f")


sdf = pd.DataFrame.spatial.from_layer(item.layers[0]) 


capEfectiva = sdf['capEfectiva'].fillna(-1)
arregloCapEfectiva = capEfectiva.values



creationDate = pd.to_datetime(sdf['CreationDate'])
arregloCreationDate = creationDate.values



cursor1 = connection.cursor()
sql1 = 'DELETE from graficar'
cursor1.execute(sql1)
connection.commit()
print("Tabla 'graficar' vacia")


for i, j in zip(arregloCapEfectiva, arregloCreationDate):    
    print(i)
    print(j)
    
    if i != -1:
        cursor = connection.cursor()
        sql = 'INSERT INTO graficar(capacidadCarga, CreationDate) values(%s, %s)'
        val = (float(i), str(j))
        cursor.execute(sql, val)
        connection.commit()


# In[ ]:


print("Capa 1, nada")
#Nada


# In[28]:


sdf2 = pd.DataFrame.spatial.from_layer(item.layers[2]) 

#Prom_VisSend, Prom_Ancho, Area_Sendero, Cap_Fisica, CreationDate

prom_VisSend = sdf2['Prom_VisSend'].fillna(-1)
arregloProm = prom_VisSend.values



prom_ancho = sdf2['Prom_Ancho'].fillna(-1)
arregloPromAncho = prom_ancho.values


area_sendero = sdf2['Area_Sendero'].fillna(-1)
arregloAreaSendero = area_sendero.values


cap_fisica = sdf2['Cap_Fisica'].fillna(-1)
arregloCapFisica = cap_fisica.values


creationDate = pd.to_datetime(sdf2['CreationDate'])
arregloCreationDate = creationDate.values



cursor2 = connection.cursor()
sql2 = 'DELETE from tabla2'
cursor2.execute(sql2)
connection.commit()
print("Tabla 'tabla2' vacia")


for i, j, p, b, n in zip(arregloProm, arregloPromAncho, arregloAreaSendero, arregloCapFisica, arregloCreationDate):    
    print(i)
    print(j)
    print(p)
    print(b)
    print(n)
    
    
    if i != -1 or j != -1 or p != -1 or b != -1:
        cursor = connection.cursor()
        sql = 'INSERT INTO tabla2(prom_VisSend, prom_ancho, area_sendero, cap_fisica, creationDate) values(%s, %s, %s, %s, %s)'
        val = (float(i), float(j), float(p), float(b), str(n))
        cursor.execute(sql, val)
        connection.commit()
    



# In[29]:


sdf3 = pd.DataFrame.spatial.from_layer(item.layers[3]) 

#area_Erod, CreationDate


areaErod = sdf3['area_Erod'].fillna(-1)
arregloAreaErod = areaErod.values


creationDate3 = pd.to_datetime(sdf3['CreationDate'])
arregloCreationDate3 = creationDate3.values



cursor3 = connection.cursor()
sql3 = 'DELETE from tabla3'
cursor3.execute(sql3)
connection.commit()
print("Tabla 'tabla3' vacia")


for i, j in zip(arregloAreaErod, arregloCreationDate3):    
    print(i)
    print(j)
    
    if i != -1:
        cursor = connection.cursor()
        sql = 'INSERT INTO tabla3(areaErod, creationDate) values(%s, %s)'
        val = (float(i), str(j))
        cursor.execute(sql, val)
        connection.commit()    


# In[31]:



sdf4 = pd.DataFrame.spatial.from_layer(item.layers[4]) 

#verificar_pend_A, long_Acces, CreationDate


verPendA = sdf4['verificar_pend_A'].fillna(-1)
arregloVerPendA = verPendA.values


longAcc = sdf4['long_Acces'].fillna(-1)
arregloVerPendA = longAcc.values


creationDate4 = pd.to_datetime(sdf4['CreationDate'])
arregloCreationDate4 = creationDate4.values



cursor4 = connection.cursor()
sql4 = 'DELETE from tabla4'
cursor4.execute(sql4)
connection.commit()
print("Tabla 'tabla4' vacia")



for i, j, p in zip(arregloVerPendA, arregloVerPendA, arregloCreationDate4):    
   print(i)
   print(j)
   print(p)
   
   if i != -1 or j != -1:
       cursor = connection.cursor()
       sql = 'INSERT INTO tabla4(verificar_pend_A, long_Acces, CreationDate) values(%s, %s, %s)'
       val = (float(i), float(j), str(p))
       cursor.execute(sql, val)
       connection.commit()    


# In[38]:



print("Capa 5 no hay")


# In[32]:


sdf6 = pd.DataFrame.spatial.from_layer(item.layers[6]) 


#dias_especiales, CreationDate

dias_especiales = sdf6['dias_especiales'].fillna(-1)
arregloDiasEspeciales = dias_especiales.values


creationDate6 = pd.to_datetime(sdf6['CreationDate'])
arregloCreationDate6 = creationDate6.values



cursor6 = connection.cursor()
sql6 = 'DELETE from tabla6'
cursor6.execute(sql6)
connection.commit()
print("Tabla 'tabla6' vacia")



for i, p in zip(arregloDiasEspeciales, arregloCreationDate6):    
    print(i)
    print(p)
    
    if i != -1:
        cursor = connection.cursor()
        sql = 'INSERT INTO tabla6(dias_especiales, creationDate) values(%s, %s)'
        val = (float(i),  str(p))
        cursor.execute(sql, val)
        connection.commit() 


# In[35]:


sdf7 = pd.DataFrame.spatial.from_layer(item.layers[7]) 


#sum_flora, CreationDate

sumFlora = sdf7['sum_flora'].fillna(-1)
arregloSumFlora = sumFlora.values


creationDate7 = pd.to_datetime(sdf7['CreationDate'])
arregloCreationDate7 = creationDate7.values


cursor7 = connection.cursor()
sql7 = 'DELETE from tabla7'
cursor7.execute(sql7)
connection.commit()
print("Tabla 'tabla7' vacia")



for i, p in zip(arregloSumFlora, arregloCreationDate7):    
    print(i)
    print(p)
    
    if i != -1:
        cursor = connection.cursor()
        sql = 'INSERT INTO tabla7(sum_flora, creationDate) values(%s, %s)'
        val = (float(i),  str(p))
        cursor.execute(sql, val)
        connection.commit() 


# In[ ]:





# In[26]:


print("Fín de la ejecución")


# In[ ]:




