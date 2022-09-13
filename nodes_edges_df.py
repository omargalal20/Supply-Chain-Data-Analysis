from argparse import _ActionsContainer
from itertools import product
from select import select
from turtle import distance
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import numpy as np
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
        self.edges_df_edges_as_nodes = pd.DataFrame(columns=['From', 'To', 'From_Table', 'To_Table', 'Weight', 'Distance','Edge_Name'])
        self.distance = pd.DataFrame(columns=['From','To', 'Distance'])
        self.edges_as_edges = edges_as_edges
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
        
        return self.nodes_df_edges_as_nodes, self.edges_df_edges_as_nodes

    def __create_nodes_df(self):
        for node in self.nodes:
            column_names = list(self.All_dfs[node].columns)  # get column names
            for index, row in self.All_dfs[node].iterrows():
                att = {}
                for i in range(1, len(column_names)):
                    att[column_names[i]] = self.All_dfs[node].iloc[index, i]
                newRow = [{'Label': node, 'ID': self.All_dfs[node].iloc[index, 0], 'Attributes': att}]
                tmp = pd.DataFrame(newRow)
                self.nodes_df_edges_as_nodes = pd.concat([self.nodes_df_edges_as_nodes, tmp], ignore_index=True)

    def __add_edges_to_edges_df(self):
        for edge_name in self.edges:
            foreign_keys = list(self.fk[edge_name].keys())

            from_col = foreign_keys[0]
            from_table_name = self.fk[edge_name][from_col]

            to_col = foreign_keys[-1]
            to_table_name = self.fk[edge_name][to_col]

            column_names = list(self.All_dfs[edge_name].columns)  # get column names
            for index, _ in self.All_dfs[edge_name].iterrows():
                att = {}
                from_ref_id, to_ref_id = None, None

                for i in range(1, len(column_names)):
                    column_name = column_names[i]

                    if column_name not in foreign_keys:
                        att[column_name] = self.All_dfs[edge_name].iloc[index, i]

                    else:
                        reference_id = self.All_dfs[edge_name].iloc[index, i]
                        if column_name == from_col:
                            # from_ref_id = from_df[from_df[from_df_pk] == reference_id].index[0]
                            from_ref_id = reference_id
                        else:
                            # to_ref_id = to_df[to_df[to_df_pk] == reference_id].index[0]
                            to_ref_id = reference_id

                # Adding new entry to node tabel
                newRow = [{'Label': edge_name, 'ID': self.All_dfs[edge_name].iloc[index, 0], 'Attributes': att}]
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

                # from ---> edge
                new_from_edge_row = [
                    {'From': from_node_id, 'To': edge_node_index, 'From_Table': from_table_name.capitalize(),
                     'To_Table': edge_name.capitalize()
                        , 'Weight': 100, 'Edge_Name': edge_name}]
                tmp = pd.DataFrame(new_from_edge_row)
                self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)
                # edge --->to
                new_to_edge_row = [{'From': edge_node_index, 'To': to_node_id, 'From_Table': edge_name.capitalize(),
                                    'To_Table': to_table_name.capitalize()
                                       , 'Weight': 100, 'Edge_Name': edge_name}]
                tmp = pd.DataFrame(new_to_edge_row)
                self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)

    def __add_properties_to_dfs(self):
        for property_name in self.properties:
            print(property_name)

            property_df = self.All_dfs[property_name]

            foreign_keys = list(self.fk[property_name].keys())

            fk_col = foreign_keys[0]
            referenced_table_name = self.fk[property_name][fk_col]

            column_names = list(property_df.columns)  # get column names

            for index, _ in property_df.iterrows():
                att = {}
                reference_id = None

                for i in range(1, len(column_names)):
                    column_name = column_names[i]

                    if column_name not in foreign_keys:
                        att[column_name] = property_df.iloc[index, i]

                    else:
                        # capturing foreign key value
                        reference_id = property_df.iloc[index, i]

                # Adding new entry to node tabel
                newRow = [{'Label': property_name, 'ID': property_df.iloc[index, 0], 'Attributes': att}]
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
                                                     , 'Weight': 100,'Distance':0 ,'Edge_Name': "Related_To"}]
                        tmp = pd.DataFrame(new_property_edge_row)
                        self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)

                else:
                    referenced_node_id = self.nodes_df_edges_as_nodes[(self.nodes_df_edges_as_nodes['Label'] == referenced_table_name) & (
                            self.nodes_df_edges_as_nodes['ID'] == reference_id)].index[0]

                    new_property_edge_row = [{'From': referenced_node_id, 'To': property_node_index,
                                              'From_Table': referenced_table_name.capitalize(),
                                              'To_Table': property_name.capitalize()
                                                 , 'Weight': 100,'Distance':0, 'Edge_Name': "Related_To"}]
                    tmp = pd.DataFrame(new_property_edge_row)
                    self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)

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

    def calaculateDistance(self):
        self.distance= geodesic(self.fromCoordinates,self.toCoordinates).km
        return self.distance

    # def addingDistaceToWarehouseProduct
    def __calculateNewValue(self, x, columnName):
        minMax = self.edges_df_edges_as_nodes[columnName].agg(['min', 'max']).to_numpy()
                    
        if(x != 0):
            OldRange = (minMax[1] - minMax[0])  
            NewRange = (100 - 1)  
            NewValue = (((x - minMax[0]) * NewRange) / OldRange) + 1
            return NewValue
                
        return 0

    def __calculateFinalWeightFromTransportationCost(self):
        def calculateAverage(row):
            
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
                print(row['Distance'])
                row["Weight"] = row["Weight"] - 5
                return row["Weight"]

            return 0
        
        print("*********FINAL************")
        self.edges_df_edges_as_nodes["Weight"] = self.edges_df_edges_as_nodes.apply(lambda row : calculateAverage(row),axis = 1)



        
    

    


    def __add_manufacturing_relation_to_dfs(self):
        manufacturing_df = self.All_dfs["manufacturing"]
        ware_houses_df = pd.DataFrame(columns=['From', 'To', 'From_Table', 'To_Table', 'Weight', 'Distance','Edge_Name'])
        supplier_df= pd.DataFrame(columns=['From', 'To', 'From_Table', 'To_Table', 'Weight', 'Distance','Edge_Name'])
        warehouse_supplier_df = pd.DataFrame(columns=["Product", "Warehouse", "Supplier", 'Distance','Edge_Name'])


        for i, manufacturing_row in manufacturing_df.iterrows():
            factory_id = manufacturing_row["Factory_id"]
            supplier_node_index = self.nodes_df_edges_as_nodes.query(f"(Label == 'supplier' ) and (ID == {factory_id}) ").index[0]
            product_id = manufacturing_row["Product_id"]
            product_node_index = self.nodes_df_edges_as_nodes.query(f"(Label == 'products' ) and (ID == {product_id}) ").index[0]
            warehouse_node_index = self.warehousesOfProducts(product_node_index)


            ware_house_country_name = self.nodes_df_edges_as_nodes.iloc[warehouse_node_index]['Attributes']['country']
            ware_house_city_name = self.nodes_df_edges_as_nodes.iloc[warehouse_node_index]['Attributes']['city_name']

            supplier_country_name = self.nodes_df_edges_as_nodes.iloc[supplier_node_index]['Attributes']['country']
            supplier_city_name = self.nodes_df_edges_as_nodes.iloc[supplier_node_index]['Attributes']['city_name'] 

            self.warehouseSupplierFromCoordinates(ware_house_city_name,ware_house_country_name)
            self.warehouseSupplierToCoordinates(supplier_city_name,supplier_country_name)
            self.calaculateDistance()



            # #supplier -> products
            new_supplier_edge_row = [
                {'From': supplier_node_index, 'To': product_node_index,
                    'From_Table': "supplier".capitalize(),
                    'To_Table': "products".capitalize()
                    ,'Weight': 100, 'Distance': 0, 'Edge_Name': "Related_To"}]            
            suppliertmp = pd.DataFrame(new_supplier_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, suppliertmp], ignore_index=True)
            supplier_df= pd.concat([supplier_df,suppliertmp],ignore_index=True)


            # new_warehouse_supplier_row = [
            #     {'Product': product_node_index, 'Warehouse': warehouse_node_index,
            #      'Supplier':supplier_node_index,
            #       'Distance': self.calaculateDistance(),
            #       'Edge_Name':"Related_To"}
            # ]
            # warehouse_supplier_tmp= pd.DataFrame(new_warehouse_supplier_row)
            # warehouse_supplier_df = pd.concat([warehouse_supplier_df,warehouse_supplier_tmp],ignore_index=True)


            self.edges_df_edges_as_nodes.loc[self.edges_df_edges_as_nodes["From"]==warehouse_node_index,"Distance"]=self.calaculateDistance()

        self.edges_df_edges_as_nodes["Distance"] = self.edges_df_edges_as_nodes["Distance"].apply(lambda x: self.__calculateNewValue(x, "Distance"))

        self.__calculateFinalWeightFromTransportationCost()

        self.edges_df_edges_as_nodes.to_csv("edges.csv")

    
        



        



        


            
        




    def warehousesOfProducts(self,prodId):
        for r, row in self.edges_df_edges_as_nodes.iterrows():
            product_node_index = row['To']
            warehouse_node_index = row['From']
            if prodId == product_node_index:
                return warehouse_node_index





    def __add_internal_orders_to_dfs(self):
        ss_internal_orders_df = self.All_dfs["ssintorders"]
        for i, ss_internal_order_row in ss_internal_orders_df.iterrows():

            att = {}
            for column_name in ss_internal_orders_df:
                if (column_name not in self.fk["ssintorders"].keys() and column_name != self.pk["ssintorders"]):
                    att[column_name] = ss_internal_order_row[column_name]

            newRow = [{'Label': "ssintorders", 'ID': ss_internal_orders_df.iloc[i, 0], 'Attributes': att}]

            tmp = pd.DataFrame(newRow)
            self.nodes_df_edges_as_nodes = pd.concat([self.nodes_df_edges_as_nodes, tmp], ignore_index=True)

            internal_order_index = len(self.nodes_df_edges_as_nodes) - 1

            internal_shipment_id = ss_internal_order_row["IntShip_id"]
            internal_shipment_node_index = \
                self.nodes_df_edges_as_nodes.query(f"(Label == 'ssintship' ) and (ID == {internal_shipment_id}) ").index[0]

            product_id = ss_internal_order_row["prod_id"]
            product_node_index = self.nodes_df_edges_as_nodes.query(f"(Label == 'products' ) and (ID == {product_id}) ").index[0]

            # from ---> edge
            new_from_edge_row = [
                {'From': internal_shipment_node_index, 'To': internal_order_index,
                 'From_Table': "ssintship".capitalize(),
                 'To_Table': "ssintorders".capitalize()
                    , 'Weight': 100, 'Edge_Name': "Order"}]
            tmp = pd.DataFrame(new_from_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)
            
            # edge --->to
            new_to_edge_row = [
                {'From': internal_order_index, 'To': product_node_index, 'From_Table': "ssintorders".capitalize(),
                 'To_Table': "products".capitalize()
                    , 'Weight': 100, 'Edge_Name': "Orders_Prodcut"}]
            tmp = pd.DataFrame(new_to_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)




