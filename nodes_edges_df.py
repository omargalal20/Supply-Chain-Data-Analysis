import pandas as pd
from AddTransportationWeightsToEdges import AddTransportationWeightsToEdges
from selenium import webdriver
from selenium.webdriver.edge.service import Service
import os.path
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import math

class nodes_edges_dfs:

    def __init__(self, nodes, edges, properties, pk, fk, ref_in, All_dfs,edges_as_edges):
        self.nodes = nodes
        self.edges = edges
        self.properties = properties
        self.pk = pk
        self.fk = fk
        self.ref_in = ref_in
        self.All_dfs = All_dfs
        self.geolocator = Nominatim(user_agent="trialApp")
        self.nodesTable = pd.DataFrame(columns=['Label', 'ID', 'Attributes'])
        self.edgesTable = pd.DataFrame(columns=['From_Node_ID', 'To_Node_ID', 'order/service'])
        self.nodes_df_edges_as_nodes = pd.DataFrame(columns=['Label', 'ID', 'Attributes'])
        self.edges_df_edges_as_nodes = pd.DataFrame(columns=['From', 'To', 'From_Table', 'To_Table', 'Weight', 'Transportation_Cost', 'Transportation_Distance', 'Transportation_Duration','Transportation_Type', 'Rental price', 'price', 'profit_margin (%)', 'market_share (%)', 'Annual_sales', 'Distance','Edge_Name'])

        self.edges_as_edges = edges_as_edges

        self.distanceAndDuration_df = pd.DataFrame(columns=['From','To', 'Distance', 'Duration', 'typeOfTransportation'])

        self.create_nodes_and_edges_df()
        self.fromCoordinates = set()
        self.toCoordinates= set()
        self.distance = 0

    def create_nodes_and_edges_df(self):
        if(self.edges_as_edges):
            print('TRUEEEE')
            self.__prepare_graph_edges_as_edges()
            return self.nodesTable,self.edgesTable
        else:
            print('FALSEEE')
            self.__prepare_graph_edges_as_nodes()
            return self.nodes_df_edges_as_nodes,self.edges_df_edges_as_nodes

    def __prepare_graph_edges_as_edges(self):
        self.nodesTable = self.__add_nodes()
        self.edgesTable = self.__add_edges()

    # Create nodes table
    def __add_nodes(self):
        nodesTB = pd.DataFrame(columns=['Label', 'ID', 'Attributes'])
        
        allDfsKeys = list(self.All_dfs.keys())

        for node in self.nodes:
            dfNumpy =  self.All_dfs[node].to_numpy()
            allDfsKeys.remove(node)
            for row in dfNumpy:
                newRow = [{'Label': node, 'ID': row[0] , 'Attributes': row[1:]}]
                tmp = pd.DataFrame(newRow)
                nodesTB = pd.concat([nodesTB, tmp], ignore_index=True)
        return nodesTB

    # 
    def __convert_prop(self, edge_key, pk_value):
        for referenced_table_name in self.ref_in[edge_key]:
            referenced_table = self.All_dfs[referenced_table_name]
            fk_of_ref_table = self.fk[referenced_table_name]
            for foreign_key in fk_of_ref_table:
                if fk_of_ref_table[foreign_key] == edge_key:
                    # referenced_table[referenced_table[foreign_key]== id]
                    all_occurances_df = referenced_table[referenced_table[foreign_key] == pk_value].drop([foreign_key],
                                                                                                         axis=1)
                    return all_occurances_df.to_dict('records')

    # Create edges table
    def __add_edges(self):
        edgesTB = pd.DataFrame(columns=['From_Node_ID', 'To_Node_ID', 'order/service'])
        for edge in self.edges:
            df = self.All_dfs[edge]  # df of the cur edge
            for row in range(len(df)):
                label = list(self.fk[edge].keys())
                from_id = self.nodesTable[
                    (self.nodesTable["Label"] == self.fk[edge][label[0]]) & (df.loc[row, label[0]] == self.nodesTable["ID"])]
                from_id = from_id.index[0]
                to_id = self.nodesTable[
                    (self.nodesTable["Label"] == self.fk[edge][label[1]]) & (df.loc[row, label[1]] == self.nodesTable["ID"])]
                to_id = to_id.index[0]
                pk_col = self.pk[edge]
                # primary_key = df[pk_col].iloc[row]
                primary_key = df.loc[row, pk_col]
                att = self.__convert_prop(edge, primary_key)
                newRow = [{'From_Node_ID': from_id, 'To_Node_ID': to_id, 'order/service': att}]
                tmp = pd.DataFrame(newRow)
                edgesTB = pd.concat([edgesTB, tmp], ignore_index=True)
        return edgesTB

    def __prepare_graph_edges_as_nodes(self):

        self.__create_nodes_df()
        self.__add_edges_to_edges_df()
        self.__add_properties_to_dfs()
        self.__add_manufacturing_relation_to_dfs()
        self.__add_internal_orders_to_dfs()
        self.__cal_weight()

        return self.nodes_df_edges_as_nodes, self.edges_df_edges_as_nodes

    # Method to Calculate Weight from product price, annual sales, market share, rental price of warehourse, and profit margin
    def __cal_weight(self):
        if self.edges_df_edges_as_nodes.empty and self.nodes_df_edges_as_nodes.empty:
            print('__cal_weight: DataFrame is still empty')
            return 
        print('/////////////////////////////////////////////////////////////////////////////////////////////////////////')

        avg_AnuualSales= int(pd.read_csv("DataSet/Supplier_data.csv")["Annual_sales"].mean())
        avg_MarketShare= int(pd.read_csv("DataSet/Supplier_data.csv")["market_share (%)"].mean())   

        avg_price= int(pd.read_csv("DataSet/Products_data.csv")["price"].mean())
        avg_ProfitMargin= int(pd.read_csv("DataSet/Products_data.csv")["profit_margin (%)"].mean())
            
        avg_RentalPrice= int(pd.read_csv("Dataset/warehouses_data.csv")["Rental price"].mean())
            
        # print(avg_price,avg_ProfitMargin,avg_AnuualSales,avg_MarketShare)
        myedges= self.edges_df_edges_as_nodes
        mynodes= self.nodes_df_edges_as_nodes
        costs_sup= {'Annual_sales','market_share (%)'} #Costs to search for in Attributes of From & To
        costs_product= {'price','profit_margin (%)'}
        costs_warhouse={'Rental price'}
            
            
        for index, row in myedges.iterrows():
            from_id= row['From']
            to_id= row['To']

            # attribute_from= pd.Series( mynodes.loc[from_id]['Attributes'])
            attribute_to= pd.Series(mynodes.loc[to_id]['Attributes'])

            if not attribute_to.empty:
                intersectto_sup= set(attribute_to.index).intersection(costs_sup)
                intersectto_pro= set(attribute_to.index).intersection(costs_product)
                intersectto_war= set(attribute_to.index).intersection(costs_warhouse)
                    
                # set1.union(set2, set3, set4......)
                total_to= intersectto_sup.union(intersectto_pro,intersectto_war)
                if total_to==0:
                    print("No attributes found match costs in To nodes")
                else:
                    # print(total_to)
                    # print('\n')
                    finalInt=attribute_to[total_to]
                    # print("Int of To:    ",finalInt)
                    # w= randint(1,6)
                    for i in range(0,len(finalInt)):
                        ind=finalInt.index[i]
                        val=finalInt.values[i]
                        if ind =='Annual_sales':
                            myedges.loc[index, 'Annual_sales'] = val
                            myedges.loc[index, 'Weight'] = myedges.loc[index, 'Weight']-5 if val>=avg_AnuualSales else myedges.loc[index, 'Weight']-10     
                        if ind =='market_share (%)':
                            myedges.loc[index, 'market_share (%)'] = val
                            myedges.loc[index, 'Weight'] = myedges.loc[index, 'Weight']-5 if val>=avg_MarketShare else myedges.loc[index, 'Weight']-20
                        if ind =='price':
                            myedges.loc[index, 'price'] = val
                            myedges.loc[index, 'Weight'] = myedges.loc[index, 'Weight']-30 if val>=avg_price else myedges.loc[index, 'Weight']-50
                        if ind =='profit_margin (%)':
                            myedges.loc[index, 'profit_margin (%)'] = val
                            myedges.loc[index, 'Weight'] = myedges.loc[index, 'Weight']-15 if val>=avg_ProfitMargin else myedges.loc[index, 'Weight']-30
                        if ind =='Rental price':
                            myedges.loc[index, 'Rental price'] = val
                            myedges.loc[index, 'Weight'] = myedges.loc[index, 'Weight']-40 if val>=avg_RentalPrice else myedges.loc[index, 'Weight']-80
            
        print('/////////////////////////////////////////////////////////////////////////////////////////////////////////')
        myedges= myedges.fillna(0)
        mynodes= mynodes.fillna(0)
        self.edges_df_edges_as_nodes= myedges
        self.nodes_df_edges_as_nodes= mynodes
        # myedges.to_csv('myedges.csv')
        return

    # Method to convert a number in certain range to its corresponding new value in a new range
    def __calculateNewValue(self, x, columnName):
        minMax = self.edges_df_edges_as_nodes[columnName].agg(['min', 'max']).to_numpy()
                    
        if(x != 0):
            OldRange = (minMax[1] - minMax[0])  
            NewRange = (100 - 1)  
            NewValue = (((x - minMax[0]) * NewRange) / OldRange) + 1
            return round(NewValue) / 2
                
        return 0

    # Method to apply the above function on all values in the columns that are entered to the method in the form of list
    def __adjustWeightRangeForFinalWeight(self, columnNames):

        for columnName in columnNames:
            self.edges_df_edges_as_nodes[columnName] = self.edges_df_edges_as_nodes[columnName].apply(lambda x: self.__calculateNewValue(x, columnName))

    # Method to calculate Final Weight from Transportation cost
    def __calculateFinalWeightFromTransportationCost(self):
        def calculateAverage(row):
            # return row['Weight'] + (row['Distance'] + row['Duration'] / 2)
            return (row['Transportation_Distance'] + row['Transportation_Duration'] / 2)

        self.edges_df_edges_as_nodes['Transportation_Cost'] = self.edges_df_edges_as_nodes.apply(lambda row: calculateAverage(row), axis = 1)
        self.edges_df_edges_as_nodes['Weight'] = self.edges_df_edges_as_nodes['Weight'] - self.edges_df_edges_as_nodes.apply(lambda row: calculateAverage(row), axis = 1)

    # Method Used only when web scraping is used in order to gather the distance and duration data
    def __calculateTransportationWeight(self, fromId, toId, edgeId, driver):
        typeOfTransportation = self.nodes_df_edges_as_nodes.iloc[edgeId]['Attributes']
        transportationWeight = {}
        # print(self.nodes_df_edges_as_nodes.iloc[fromId])
        # print(self.nodes_df_edges_as_nodes.iloc[toId])

        if('TransportationType' in typeOfTransportation):
            # print(typeOfTransportation)
            transportationWeight = AddTransportationWeightsToEdges(self.nodes_df_edges_as_nodes.iloc[fromId], self.nodes_df_edges_as_nodes.iloc[toId], driver, typeOfTransportation['TransportationType'])
            # print('------------------------------------------------------------')
            # print(f"Distance is {transportationWeight.distance}".encode('utf-8'))
            # print(f"Duration is {transportationWeight.duration}".encode('utf-8'))
            # print('------------------------------------------------------------')
            newRow = [{'From': transportationWeight.fromName,'To': transportationWeight.toName, 'Distance': transportationWeight.distance, 'Duration': transportationWeight.duration, 'typeOfTransportation' : typeOfTransportation['TransportationType']}]
            tmp = pd.DataFrame(newRow)
            self.distanceAndDuration_df = pd.concat([self.distanceAndDuration_df, tmp], ignore_index=True)

    # Method to create the nodes dataframe from the dataframes that are determined as nodes
    def __create_nodes_df(self):
        for node in self.nodes:
            column_names = list(self.All_dfs[node].columns)  # get column names
            # for index, row in self.All_dfs[node].iterrows():
            #     att = {}
            #     for i in range(1, len(column_names)):
            #         att[column_names[i]] = self.All_dfs[node].iloc[index, i]
            #     newRow = [{'Label': node, 'ID': self.All_dfs[node].iloc[index, 0], 'Attributes': att}]
            #     tmp = pd.DataFrame(newRow)
            #     self.nodes_df_edges_as_nodes = pd.concat([self.nodes_df_edges_as_nodes, tmp], ignore_index=True)
            dfNumpy =  self.All_dfs[node].to_numpy()
            for index in range(dfNumpy.shape[0]):
                att = {}
                for i in range(1, len(column_names)):
                    att[column_names[i]] = dfNumpy[index][i]
                newRow = [{'Label': node, 'ID': dfNumpy[index][0], 'Attributes': att}]
                tmp = pd.DataFrame(newRow)
                self.nodes_df_edges_as_nodes = pd.concat([self.nodes_df_edges_as_nodes, tmp], ignore_index=True)

    # Method to create the edges dataframe from the dataframes that are determined as edges, and includes the calculation of the transportation cost along with the gathering of the distance and duration data
    def __add_edges_to_edges_df(self):
        if(os.path.exists('distanceAndDurationData.csv')):
            firstTimeToWriteToFile = False
        else:
            driver = webdriver.Edge(service=Service("./msedgedriver.exe"))
            driver.get("https://www.searates.com/services/distances-time/")

            driver.implicitly_wait(5)
            firstTimeToWriteToFile = True

        for edge_name in self.edges:

            foreign_keys = list(self.fk[edge_name].keys())

            from_col = foreign_keys[0]
            from_table_name = self.fk[edge_name][from_col]

            to_col = foreign_keys[-1]
            to_table_name = self.fk[edge_name][to_col]

            print(f"/////////// {edge_name} //////////////////")

            column_names = list(self.All_dfs[edge_name].columns)  # get column names
            # print(f'Size {self.All_dfs[edge_name].shape}')
            # for index, _ in self.All_dfs[edge_name].iterrows():
            dfNumpy =  self.All_dfs[edge_name].to_numpy()
            for index in range(dfNumpy.shape[0]):
                att = {}
                from_ref_id, to_ref_id = None, None

                for i in range(1, len(column_names)):
                    column_name = column_names[i]

                    if column_name not in foreign_keys:
                        att[column_name] = dfNumpy[index][i]

                    else:
                        reference_id = dfNumpy[index][i]
                        if column_name == from_col:
                            # from_ref_id = from_df[from_df[from_df_pk] == reference_id].index[0]
                            from_ref_id = reference_id
                        else:
                            # to_ref_id = to_df[to_df[to_df_pk] == reference_id].index[0]
                            to_ref_id = reference_id

                # Adding new entry to node tabel
                newRow = [{'Label': edge_name, 'ID': dfNumpy[index][0], 'Attributes': att}]
                tmp = pd.DataFrame(newRow)
                self.nodes_df_edges_as_nodes = pd.concat([self.nodes_df_edges_as_nodes, tmp], ignore_index=True)
                edge_node_index = len(self.nodes_df_edges_as_nodes) - 1
                # print(nodes_df.iloc[len(nodes_df)-1],All_dfs[Edge_name].iloc[index,0] )
                # creating two edges, one from the from_node to the edge node and one from edge node to to_node
                from_node_id = \
                    self.nodes_df_edges_as_nodes[
                        (self.nodes_df_edges_as_nodes['Label'] == from_table_name) & (self.nodes_df_edges_as_nodes['ID'] == from_ref_id)].index[
                        0]        
                to_node_id = \
                    self.nodes_df_edges_as_nodes[(self.nodes_df_edges_as_nodes['Label'] == to_table_name) & (self.nodes_df_edges_as_nodes['ID'] == to_ref_id)].index[
                        0]
                    
                totalDuration = 0
                totalDistance = 0
                transportationType = ''

                if(firstTimeToWriteToFile and (edge_name not in ['internaltransactions', 'externaltransactions'])):
                    print(f'Index {index}')
                    print(f"From Row {self.nodes_df_edges_as_nodes.iloc[from_node_id]['Attributes']}".encode('utf-8'))
                    print(f"To Row {self.nodes_df_edges_as_nodes.iloc[to_node_id]['Attributes']}".encode('utf-8'))
                    self.__calculateTransportationWeight(from_node_id, to_node_id, edge_node_index, driver)
                    totalDuration += self.distanceAndDuration_df.iloc[index]['Duration']
                    totalDistance += self.distanceAndDuration_df.iloc[index]['Distance']
                    transportationType += durationAndDistanceData.iloc[index]['typeOfTransportation']
                elif(edge_name not in ['internaltransactions', 'externaltransactions']):
                    durationAndDistanceData = pd.read_csv('distanceAndDurationData.csv')
                    totalDuration += durationAndDistanceData.iloc[index]['Duration']
                    totalDistance += durationAndDistanceData.iloc[index]['Distance']
                    transportationType += durationAndDistanceData.iloc[index]['typeOfTransportation']

                # from ---> edge
                new_from_edge_row = [
                    {'From': from_node_id, 'To': edge_node_index, 'From_Table': from_table_name.capitalize(),
                     'To_Table': edge_name.capitalize()
                        , 'Weight': 100, 'Transportation_Distance': totalDistance, 'Transportation_Duration': totalDuration, 
                        'Transportation_Type': 'N/A' if edge_name in ['internaltransactions', 'externaltransactions'] else transportationType, 'Edge_Name': edge_name}]
                tmp = pd.DataFrame(new_from_edge_row)
                self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)
                # edge --->to
                new_to_edge_row = [{'From': edge_node_index, 'To': to_node_id, 'From_Table': edge_name.capitalize(),
                                    'To_Table': to_table_name.capitalize()
                                       , 'Weight': 100, 'Transportation_Distance': totalDistance, 'Transportation_Duration': totalDuration, 
                                       'Transportation_Type': 'N/A' if edge_name in ['internaltransactions', 'externaltransactions'] else transportationType,'Edge_Name': edge_name}]
                tmp = pd.DataFrame(new_to_edge_row)
                self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)

            if(firstTimeToWriteToFile and edge_name not in ['internaltransactions', 'externaltransactions']):
                self.distanceAndDuration_df.to_csv('distanceAndDurationData.csv',index=False)
                print('Writing')
                # if (os.path.exists('distanceAndDurationData.csv') == True and edge_name not in ['internaltransactions', 'externaltransactions']):
                #     self.distanceAndDuration_df.to_csv('distanceAndDurationData.csv', mode='a', index=False, header=False)
                # elif (os.path.exists('distanceAndDurationData.csv') == False and edge_name not in ['internaltransactions', 'externaltransactions']):
                #     self.distanceAndDuration_df.to_csv('distanceAndDurationData.csv',index=False)

        if(firstTimeToWriteToFile):
            driver.close()
            driver.quit()
        self.__adjustWeightRangeForFinalWeight(['Transportation_Distance', 'Transportation_Duration'])
        self.__calculateFinalWeightFromTransportationCost()
        print('Finish Edges Method')

    # Method to add the dataframes that are determined as properties to the nodes dataframe
    def __add_properties_to_dfs(self):
        for property_name in self.properties:
            # print(property_name)

            property_df = self.All_dfs[property_name]

            foreign_keys = list(self.fk[property_name].keys())

            fk_col = foreign_keys[0]
            referenced_table_name = self.fk[property_name][fk_col]

            column_names = list(property_df.columns)  # get column names

            # for index, _ in property_df.iterrows():
            dfNumpy =  property_df.to_numpy()
            for index in range(dfNumpy.shape[0]):
                att = {}
                reference_id = None

                for i in range(1, len(column_names)):
                    column_name = column_names[i]

                    if column_name not in foreign_keys:
                        att[column_name] = dfNumpy[index][i]

                    else:
                        # capturing foreign key value
                        reference_id = dfNumpy[index][i]

                # Adding new entry to node tabel
                newRow = [{'Label': property_name, 'ID': dfNumpy[index][0], 'Attributes': att}]
                tmp = pd.DataFrame(newRow)
                self.nodes_df_edges_as_nodes = pd.concat([self.nodes_df_edges_as_nodes, tmp], ignore_index=True)
                property_node_index = len(self.nodes_df_edges_as_nodes) - 1
                # print(nodes_df.iloc[len(nodes_df)-1],property_df.iloc[index,0] )

                # creating two edges, one from the from_node to the edge node and one from edge node to to_node
                if isinstance(reference_id, list):
                    for list_element_id in reference_id:
                        referenced_node_id = self.nodes_df_edges_as_nodes[(self.nodes_df_edges_as_nodes['Label'] == referenced_table_name) & (
                                self.nodes_df_edges_as_nodes['ID'] == list_element_id)].index[0]

                        new_property_edge_row = [{'From': referenced_node_id, 'To': property_node_index,
                                                  'From_Table': referenced_table_name.capitalize(),
                                                  'To_Table': property_name.capitalize()
                                                     , 'Weight': 100, 'Transportation_Type': 'N/A', 'Distance': 0,'Edge_Name': "Related_To"}]
                        tmp = pd.DataFrame(new_property_edge_row)
                        self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)

                else:
                    referenced_node_id = self.nodes_df_edges_as_nodes[(self.nodes_df_edges_as_nodes['Label'] == referenced_table_name) & (
                            self.nodes_df_edges_as_nodes['ID'] == reference_id)].index[0]

                    new_property_edge_row = [{'From': referenced_node_id, 'To': property_node_index,
                                              'From_Table': referenced_table_name.capitalize(),
                                              'To_Table': property_name.capitalize()
                                                 , 'Weight': 100, 'Transportation_Type': 'N/A', 'Distance': 0,'Edge_Name': "Related_To"}]
                    tmp = pd.DataFrame(new_property_edge_row)
                    self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)
        print('Finish Properties Method')

    # Method to relate the products to the suppliers that produce a certain product
    def warehouseSupplierFromCoordinates(self,city_name,country_name):
        try:
            if(city_name == 'Unknown'):
                location = self.geolocator.geocode(country_name)    
                self.fromCoordinates = (location.latitude, location.longitude)
            else:
                location = self.geolocator.geocode(city_name+", "+country_name)
                self.fromCoordinates = (location.latitude, location.longitude)
        except AttributeError:
                self.fromCoordinates = (0.0, 0.0)

    def warehouseSupplierToCoordinates(self,city_name,country_name):      
        try:
            if(city_name == 'Unknown'):
                location = self.geolocator.geocode(country_name)    
                self.toCoordinates = (location.latitude, location.longitude)
            else:
                location = self.geolocator.geocode(city_name+", "+country_name)
                self.toCoordinates = (location.latitude, location.longitude)

        except AttributeError:
                self.toCoordinates = (0.0, 0.0)

    def calculateDistanceBetweenSupplierAndWarehouse(self):
        self.distance= geodesic(self.fromCoordinates,self.toCoordinates).km
        return self.distance

    def __calculateFinalWeightForWarehouseSupplier(self):
        def calculateWeight(row):
            
            if (math.isnan(row['Distance'])):
                row["Weight"] = row["Weight"] - 0
                return row["Weight"]
            elif row['Distance'] == 0:
                row["Weight"] = row["Weight"] - 0
                return row["Weight"]
            elif 1 <= row['Distance'] < 20 :
                row["Weight"] = row["Weight"] - 25
                return row["Weight"]
            elif 20 <= row['Distance'] < 40 :
                row["Weight"] = row["Weight"] - 20
                return row["Weight"]
            elif 40 <= row['Distance'] < 60 :
                row["Weight"] = row["Weight"] - 15
                return row["Weight"]
            elif 60 <= row['Distance'] < 80 :
                row["Weight"] = row["Weight"] - 10
                return row["Weight"]        
            elif 80 <= row['Distance'] <= 100:
                row["Weight"] = row["Weight"] - 5
                return row["Weight"]

            return 0

        self.edges_df_edges_as_nodes["Weight"] = self.edges_df_edges_as_nodes.apply(lambda row : calculateWeight(row),axis = 1)

    def __calculateNewValueWarehouse(self, x, columnName):
        minMax = self.edges_df_edges_as_nodes[columnName].agg(['min', 'max']).to_numpy()
                        
        if(x != 0):
            OldRange = (minMax[1] - minMax[0])  
            NewRange = (100 - 1)  
            NewValue = (((x - minMax[0]) * NewRange) / OldRange) + 1
            return NewValue
                    
        return 0
    
    def warehousesOfProducts(self,prodId):
        for _, row in self.edges_df_edges_as_nodes.iterrows():
            product_node_index = row['To']
            warehouse_node_index = row['From']
            if prodId == product_node_index:
                return warehouse_node_index

    def __add_manufacturing_relation_to_dfs(self):
        manufacturing_df = self.All_dfs["manufacturing"]
        manufacturing_columns = list(self.All_dfs["manufacturing"].columns)
        factory_id_index = manufacturing_columns.index("Factory_id")
        product_id_index = manufacturing_columns.index("Product_id")
        supplier_df= pd.DataFrame(columns=['From', 'To', 'From_Table', 'To_Table', 'Weight', 'Distance', 'Edge_Name'])

        # for _, manufacturing_row in manufacturing_df.iterrows():
        dfNumpy =  manufacturing_df.to_numpy()
        for index in range(dfNumpy.shape[0]):
            factory_id = dfNumpy[index][factory_id_index]
            supplier_node_index = self.nodes_df_edges_as_nodes.query(f"(Label == 'supplier' ) and (ID == {factory_id}) ").index[0]

            product_id = dfNumpy[index][product_id_index]
            product_node_index = self.nodes_df_edges_as_nodes.query(f"(Label == 'products' ) and (ID == {product_id}) ").index[0]
            warehouse_node_index = self.warehousesOfProducts(product_node_index)


            ware_house_country_name = self.nodes_df_edges_as_nodes.iloc[warehouse_node_index]['Attributes']['country']
            ware_house_city_name = self.nodes_df_edges_as_nodes.iloc[warehouse_node_index]['Attributes']['city_name']

            supplier_country_name = self.nodes_df_edges_as_nodes.iloc[supplier_node_index]['Attributes']['country']
            supplier_city_name = self.nodes_df_edges_as_nodes.iloc[supplier_node_index]['Attributes']['city_name'] 

            #self.warehouseSupplierFromCoordinates(ware_house_city_name,ware_house_country_name)
            #self.warehouseSupplierToCoordinates(supplier_city_name,supplier_country_name)
            #self.calaculateDistance()

            # #supplier -> products
            new_supplier_edge_row = [
                {'From': supplier_node_index, 'To': product_node_index,
                    'From_Table': "supplier".capitalize(),
                    'To_Table': "products".capitalize()
                    ,'Weight': 100, 'Distance': 0, 'Edge_Name': "Related_To"}]            
            suppliertmp = pd.DataFrame(new_supplier_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, suppliertmp], ignore_index=True)
            supplier_df= pd.concat([supplier_df,suppliertmp],ignore_index=True)

            #self.edges_df_edges_as_nodes.loc[self.edges_df_edges_as_nodes["From"]==warehouse_node_index,"Distance"]=self.calaculateDistance()
            self.edges_df_edges_as_nodes.loc[self.edges_df_edges_as_nodes["From"]==warehouse_node_index,"Distance"]= 100000
        self.edges_df_edges_as_nodes["Distance"] = self.edges_df_edges_as_nodes["Distance"].apply(lambda x: self.__calculateNewValueWarehouse(x, "Distance"))

        self.__calculateFinalWeightForWarehouseSupplier()

        # self.edges_df_edges_as_nodes.to_csv("edges.csv")
        print('Finish Manufacturing')


    def __add_internal_orders_to_dfs(self):
        ss_internal_orders_df = self.All_dfs["ssintorders"]

        # for i, ss_internal_order_row in ss_internal_orders_df.iterrows():
        dfNumpy =  ss_internal_orders_df.to_numpy()
        for index in range(dfNumpy.shape[0]):
            att = {}
            for column_name in ss_internal_orders_df:
                if (column_name not in self.fk["ssintorders"].keys() and column_name != self.pk["ssintorders"]):
                    att[column_name] =  dfNumpy[index][list(ss_internal_orders_df.columns).index(column_name)]
                    # att[column_name] = ss_internal_order_row[column_name]

            # newRow = [{'Label': "ssintorders", 'ID': ss_internal_orders_df.iloc[i, 0], 'Attributes': att}]
            newRow = [{'Label': "ssintorders", 'ID': dfNumpy[index][0], 'Attributes': att}]

            tmp = pd.DataFrame(newRow)
            self.nodes_df_edges_as_nodes = pd.concat([self.nodes_df_edges_as_nodes, tmp], ignore_index=True)

            internal_order_index = len(self.nodes_df_edges_as_nodes) - 1

            internal_shipment_id = dfNumpy[index][list(ss_internal_orders_df.columns).index("IntShip_id")]
            internal_shipment_node_index = \
                self.nodes_df_edges_as_nodes.query(f"(Label == 'ssintship' ) and (ID == {internal_shipment_id}) ").index[0]

            product_id = dfNumpy[index][list(ss_internal_orders_df.columns).index("prod_id")]
            product_node_index = self.nodes_df_edges_as_nodes.query(f"(Label == 'products' ) and (ID == {product_id}) ").index[0]

            # from ---> edge
            new_from_edge_row = [
                {'From': internal_shipment_node_index, 'To': internal_order_index,
                 'From_Table': "ssintship".capitalize(),
                 'To_Table': "ssintorders".capitalize()
                    , 'Weight': 100, 'Transportation_Type': 'N/A', 'Edge_Name': "Order"}]
            tmp = pd.DataFrame(new_from_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)
            
            # edge --->to
            new_to_edge_row = [
                {'From': internal_order_index, 'To': product_node_index, 'From_Table': "ssintorders".capitalize(),
                 'To_Table': "products".capitalize()
                    , 'Weight': 100, 'Transportation_Type': 'N/A', 'Edge_Name': "Orders_Product"}]
            tmp = pd.DataFrame(new_to_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)
        print('Finish Internal Orders')




