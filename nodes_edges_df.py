import pandas as pd
from AddTransportationWeightsToEdges import AddTransportationWeightsToEdges
import threading
from selenium import webdriver
from selenium.webdriver.edge.service import Service
class nodes_edges_dfs:

    def __init__(self, nodes, edges, properties, pk, fk, ref_in, All_dfs,edges_as_edges):
        self.nodes = nodes
        self.edges = edges
        self.properties = properties
        self.pk = pk
        self.fk = fk
        self.ref_in = ref_in
        self.All_dfs = All_dfs
        self.nodesTable = pd.DataFrame(columns=['Label', 'ID', 'Attributes'])
        self.edgesTable = pd.DataFrame(columns=['From_Node_ID', 'To_Node_ID', 'order/service'])

        self.nodes_df_edges_as_nodes = pd.DataFrame(columns=['Label', 'ID', 'Attributes'])
        self.edges_df_edges_as_nodes = pd.DataFrame(columns=['From', 'To', 'From_Table', 'To_Table', 'Weight', 'Distance', 'Edge_Name'])
        self.edges_as_edges = edges_as_edges

        self.distanceAndDuration_df = pd.DataFrame(columns=['From','To', 'Distance', 'Duration'])

        self.create_nodes_and_edges_df()

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

        # t1 = threading.Thread(target=self.__create_nodes_df)
        # t1.start()
        # t1.join()
        
        # t2 = threading.Thread(target=self.__add_edges_to_edges_df)
        # t2.start()
        # t2.join()
        
        # t3 = threading.Thread(target=self.__add_properties_to_dfs)
        # t3.start()
        # t3.join()
        
        # t4 = threading.Thread(target=self.__add_manufacturing_relation_to_dfs)
        # t4.start()
        # t4.join()
        
        # t5 = threading.Thread(target=self.__add_internal_orders_to_dfs)
        # t5.start()
        # t5.join()
        
        # t6 = threading.Thread(target=self.adjustWeightRangeForFinalWeight, args=(['Distance']))
        # t6.start()
        # t6.join()
        
        self.__create_nodes_df()
        self.__add_edges_to_edges_df()
        self.__add_properties_to_dfs()
        self.__add_manufacturing_relation_to_dfs()
        self.__add_internal_orders_to_dfs()
        self.adjustWeightRangeForFinalWeight(['Distance'])

        return self.nodes_df_edges_as_nodes, self.edges_df_edges_as_nodes

    def calculateNewValue(self, x, columnName):
        minMax = self.edges_df_edges_as_nodes[columnName].agg(['min', 'max']).to_numpy()
        # print(f"Minmax is {minMax}")
                    
        if(x != 0):
            OldRange = (minMax[1] - minMax[0])  
            NewRange = (100 - 1)  
            NewValue = (((x - minMax[0]) * NewRange) / OldRange) + 1
            return round(NewValue)
                
        return 0


    def adjustWeightRangeForFinalWeight(self, columnNames):
        for columnName in columnNames:
            self.edges_df_edges_as_nodes[columnName] = self.edges_df_edges_as_nodes[columnName].apply(lambda x: self.calculateNewValue(x, columnName))

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
    
    def calculateTransportationWeight(self, fromId, toId, edgeId, driver):
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

            newRow = [{'From': transportationWeight.fromName,'To': transportationWeight.toName, 'Distance': transportationWeight.distance, 'Duration': transportationWeight.duration}]
            tmp = pd.DataFrame(newRow)
            self.distanceAndDuration_df = pd.concat([self.distanceAndDuration_df, tmp], ignore_index=True)
            print(f'{self.distanceAndDuration_df}'.encode('utf-8'))

    def __add_edges_to_edges_df(self):

        driver = webdriver.Edge(service=Service("./msedgedriver.exe"))
        driver.get("https://www.searates.com/services/distances-time/")

        driver.implicitly_wait(5)

        for edge_name in self.edges:
            foreign_keys = list(self.fk[edge_name].keys())

            from_col = foreign_keys[0]
            from_table_name = self.fk[edge_name][from_col]

            to_col = foreign_keys[-1]
            to_table_name = self.fk[edge_name][to_col]

            print("/////////////////////////////")
            # print(f"From Table {from_table_name}")
            # print(f"To Table {to_table_name}")

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
                # print(f"From Row {self.nodes_df_edges_as_nodes.iloc[from_node_id].to_string()}")
                to_node_id = \
                    self.nodes_df_edges_as_nodes[(self.nodes_df_edges_as_nodes['Label'] == to_table_name) & (self.nodes_df_edges_as_nodes['ID'] == to_ref_id)].index[
                        0]
                # print(f"To Row {self.nodes_df_edges_as_nodes.iloc[to_node_id].to_string()}")

                # t1 = threading.Thread(target=self.calculateTransportationWeight, args=(from_node_id, to_node_id, edge_node_index, driver))
                # t1.start()
                # t1.join()

                self.calculateTransportationWeight(from_node_id, to_node_id, edge_node_index, driver)

                # from ---> edge
                new_from_edge_row = [
                    {'From': from_node_id, 'To': edge_node_index, 'From_Table': from_table_name.capitalize(),
                     'To_Table': edge_name.capitalize()
                        , 'Weight': 42, 'Distance': 0, 'Edge_Name': edge_name}]
                tmp = pd.DataFrame(new_from_edge_row)
                self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)
                # edge --->to
                new_to_edge_row = [{'From': edge_node_index, 'To': to_node_id, 'From_Table': edge_name.capitalize(),
                                    'To_Table': to_table_name.capitalize()
                                       , 'Weight': 42, 'Distance': 0, 'Edge_Name': edge_name}]
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
                reference_id = None;

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
                                                     , 'Weight': 42, 'Distance': 0, 'Edge_Name': "Related_To"}]
                        tmp = pd.DataFrame(new_property_edge_row)
                        self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)

                else:
                    referenced_node_id = self.nodes_df_edges_as_nodes[(self.nodes_df_edges_as_nodes['Label'] == referenced_table_name) & (
                            self.nodes_df_edges_as_nodes['ID'] == reference_id)].index[0]

                    new_property_edge_row = [{'From': referenced_node_id, 'To': property_node_index,
                                              'From_Table': referenced_table_name.capitalize(),
                                              'To_Table': property_name.capitalize()
                                                 , 'Weight': 42, 'Distance': 0, 'Edge_Name': "Related_To"}]
                    tmp = pd.DataFrame(new_property_edge_row)
                    self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)

    def __add_manufacturing_relation_to_dfs(self):
        manufacturing_df = self.All_dfs["manufacturing"]

        for i, manufacturing_row in manufacturing_df.iterrows():
            factory_id = manufacturing_row["Factory_id"]
            supplier_node_index = self.nodes_df_edges_as_nodes.query(f"(Label == 'supplier' ) and (ID == {factory_id}) ").index[0]

            product_id = manufacturing_row["Product_id"]
            product_node_index = self.nodes_df_edges_as_nodes.query(f"(Label == 'products' ) and (ID == {product_id}) ").index[0]

            new_edge_row = [
                {'From': supplier_node_index, 'To': product_node_index, 'From_Table': "supplier".capitalize(),
                 'To_Table': "products".capitalize()
                    , 'Weight': 42, 'Distance': 0, 'Edge_Name': "Manufactures"}]
            tmp = pd.DataFrame(new_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)

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
                    , 'Weight': 42, 'Distance': 0, 'Edge_Name': "Order"}]
            tmp = pd.DataFrame(new_from_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)
            # edge --->to
            new_to_edge_row = [
                {'From': internal_order_index, 'To': product_node_index, 'From_Table': "ssintorders".capitalize(),
                 'To_Table': "products".capitalize()
                    , 'Weight': 42, 'Distance': 0, 'Edge_Name': "Orders_Product"}]
            tmp = pd.DataFrame(new_to_edge_row)
            self.edges_df_edges_as_nodes = pd.concat([self.edges_df_edges_as_nodes, tmp], ignore_index=True)



