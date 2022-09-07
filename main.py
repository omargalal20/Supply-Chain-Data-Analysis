from ReadingDataSet import ReadingDataSet
from keys import keys
from InitializingNodesAndEdges import InitializeNodesAndEdges
from nodes_edges_df import nodes_edges_dfs

dataSet = ReadingDataSet()
# Dictionary containing all dataframes
All_dfs = dataSet.All_dfs
# print(f"Df Keys: {All_dfs}")

key = keys(All_dfs)
# Dictionary indicating the column of each table that represents the primary key
All_pks = key.primaryKeyGetter()
print(f"Primary Keys: {All_pks}")
print('-------------------------')
# Dictionary indicating the column of each table that represents the foreign key and the referenced table name
All_fks = key.foreignKeyGetter()
print(f"Foreign Keys: {All_fks}")
print('-------------------------')
# Dictionary indicating which tables references the table 
All_ref_ins = key.ref_in
print(f"Ref In Keys: {All_ref_ins}")
print('-------------------------')

initializingNodesAndEdges = InitializeNodesAndEdges(All_dfs, All_fks)
nodes = initializingNodesAndEdges.nodes
print(f"Nodes: {nodes}")
print('-------------------------')
edges = initializingNodesAndEdges.edges
print(f"Edges: {edges}")
print('-------------------------')
properties = initializingNodesAndEdges.properties
print(f"Properties: {properties}")
print('-------------------------')


# True to output nodes table and edges table as a normal graph
# False to output nodes table and edges table but edges are nodes
# initialize_nodes_edges_df = nodes_edges_dfs(nodes, edges, properties, All_pks, All_fks, All_ref_ins, All_dfs, True)

# nodesTable = initialize_nodes_edges_df.nodesTable
# print("Nodes Table: ")
# # print(nodesTable)
# print('-------------------------')


# edgesTable = initialize_nodes_edges_df.edgesTable
# print("Edges Table: ")
# # print(edgesTable)
# print('-------------------------')

initialize_nodes_edges_df = nodes_edges_dfs(nodes, edges, properties, All_pks, All_fks, All_ref_ins, All_dfs, False)

nodes_df = initialize_nodes_edges_df.nodes_df_edges_as_nodes
print("Nodes DF: ")
# print(nodes_df['Label'].value_counts())
print(f"{nodes_df}".encode('utf-8'))
print('-------------------------')


edges_df = initialize_nodes_edges_df.edges_df_edges_as_nodes
print("Edges DF: ")
print(f"{edges_df.to_string()}")
edges_df.to_csv('edges_df.csv')
print('-------------------------')
 

# from selenium import webdriver
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.common.by import By
# from selenium.webdriver.edge.service import Service
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# import time
# import random
# import html

# modes = ['Sea', 'Air', 'Land']

# driver = webdriver.Edge(service=Service("./msedgedriver.exe"))
# driver.get("https://www.searates.com/services/distances-time/")

# driver.implicitly_wait(5)

# # Click On buttons

# def chooseTransportationMode(mode):
#     buttonList = driver.find_element(By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[1]/ul')

#     items = buttonList.find_elements(By.TAG_NAME, 'li')
#     if(mode == 'Sea'):
#         items[0].click()
#     elif(mode == 'Land'):
#         items[1].click()
#     else:
#         items[2].click()

# def insertFrom(country):
#     searchFrom = driver.find_element(By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[1]/div/input')
#     searchFrom.clear()
#     searchFrom.send_keys(Keys.CONTROL + "a")
#     searchFrom.send_keys(Keys.DELETE)
#     searchFrom.send_keys(country)
#     chosenFrom = WebDriverWait(driver, 5).until(
#         EC.presence_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[1]/div/div'))
#     )
#     try:
#         chosenFrom.find_elements(By.TAG_NAME, 'div')[0].click()
#     except:
#         return insertFrom(random.choice(countries))
#     finally:
#         return searchFrom.get_attribute('value')

# def insertTo(country):
#     searchTo = driver.find_element(By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[2]/div/input')
#     searchTo.clear()
#     searchTo.send_keys(Keys.CONTROL + "a")
#     searchTo.send_keys(Keys.DELETE)
#     searchTo.send_keys(country)
#     chosenTo = WebDriverWait(driver, 5).until(
#         EC.presence_of_element_located((By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/div[2]/div[2]/div/div'))
#     )
#     try:
#         chosenTo.find_elements(By.TAG_NAME, 'div')[0].click()
#     except:
#         return insertTo(random.choice(countries))
#     finally:
#         return searchTo.get_attribute('value')

# def getResults(countryFrom, countryTo):
#     # Click Search
#     searchButton = driver.find_element(By.XPATH, '//*[@id="distance__time-app"]/div/form/div[1]/button')
#     searchButton.click()
#     # Get Results
#     while(True):
#         try:
#             results = driver.find_element(By.XPATH, '//*[@id="panel"]/div[2]/ul')
#             resultsList = results.find_elements(By.TAG_NAME, 'li')
#             # print(countryFrom.split(',')[0])
#             # print(resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(','))
#             # print(countryTo.split(',')[0])
#             # print(resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(','))

#             if((countryFrom.split(',')[0] in [html.unescape(x.lstrip(' ')) for x in resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')]) and (countryTo.split(',')[0] in [html.unescape(x.lstrip(' ')) for x in resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')])):
#                 break
#         except:
#             try:
#                 results = WebDriverWait(driver, 1).until(
#                     EC.presence_of_element_located((By.XPATH, '//*[@id="panel"]/div[2]/ul'))
#                 )
#                 resultsList = results.find_elements(By.TAG_NAME, 'li')
#                 if((countryFrom.split(',')[0] in [html.unescape(x.lstrip(' ')) for x in resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')]) and (countryTo.split(',')[0] in [html.unescape(x.lstrip(' ')) for x in resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML").split(',')])):
#                     break
#             except:
#                 results = 'No Path'
#                 print(results)
#                 break
    
#     totalDuration = 0
#     totalDistance = 0

#     if(results != 'No Path'):
#         print(resultsList[0].find_elements(By.TAG_NAME, 'span')[0].get_attribute("innerHTML"))  
#         print('------------------')   
#         for i in range(len(resultsList)):
#             # print(resultsList[i].get_attribute("innerHTML"))
#             if(i != (len(resultsList) - 1)):
#                 try:
#                     distance = resultsList[i].find_elements(By.TAG_NAME, 'p')[0].get_attribute('innerHTML').split(',')
#                     totalDistance +=  float(distance[1].lstrip(' ').split(' ')[0].lstrip('('))
#                 except:
#                     totalDistance += 0

#                 duration = (resultsList[i].find_elements(By.TAG_NAME, 'span')[1].get_attribute('innerHTML')).split(' ')
#                 if(len(duration) == 6):
#                     totalDuration += (int(duration[0]) * 24) + int(duration[2])
#                 else:
#                     if(duration[0] == 'an'):
#                         totalDuration += 1
#                     else:
#                         totalDuration += int(duration[0])
#         print(f"Total Distance {totalDistance}")
#         print(f"Total Duration {totalDuration}")
#         print('------------------')
#         print(resultsList[-1].find_elements(By.TAG_NAME, 'span')[0].get_attribute('innerHTML')) 
        
# for i in range(100):
#     random_country1 = random.choice(countries)
#     random_country2 = random.choice(countries)
#     chooseTransportationMode(random.choice(modes))
#     time.sleep(1)
#     countryFromValue = insertFrom(random_country1[1])
#     time.sleep(1)
#     countryToValue = insertTo(random_country2[1])
#     time.sleep(1)
#     getResults(countryFromValue, countryToValue)
#     time.sleep(1)
#     print('Finish loop')

# # chooseTransportationMode('Sea')
# # time.sleep(1)
# # countryFromValue = insertFrom('Brazil')
# # time.sleep(1)
# # countryToValue = insertTo('Spain')
# # time.sleep(1)
# # getResults(countryFromValue, countryToValue)

# print('------------------')
# print('Finish')
# # time.sleep(5)
# driver.close()
# driver.quit()