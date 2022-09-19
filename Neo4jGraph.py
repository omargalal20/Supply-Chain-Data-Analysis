from lib2to3.pgen2 import driver
from neo4j import GraphDatabase
from neo4j import unit_of_work


class Neo4jGraph:

    def __init__(self, nodes_df, edges_df):

        self.__DBusername = "neo4j"
        self.__DBpassword = "123"
        self.__DBuri = "bolt://localhost:7687"
        self.__driver = GraphDatabase.driver(uri=self.__DBuri, auth=(self.__DBusername, self.__DBpassword))
        self.nodes_df = nodes_df
        self.edges_df = edges_df

        self.__transaction_execution_commands = []

        self.deleteSpecificNode = {}

        self.allExistingGraphs = []

    ## draw and save graph if graph name doesn't exist in the database else print error
    def build_database(self):
        pass

    def draw_graph(self, name):
        # check if the graph name doesn't exists in the database
        if (self.ExistingGraph(name) == False):
            # draw the graph
            self.__transaction_execution_commands = []
            self.__add_delete_statement()
            self.__add_nodes_statements()
            self.__add_edges_statements()
            self.execute_transactions()
            # save the graph
            self.saveGraph(name, nodeList=['Customer', 'Products', 'Retailer', 'Supplier', 'Rcextship', 'Scextship',
                                           'Srintship', 'Ssintship', 'Facilities', 'Warehouses', 'Rcextorders',
                                           'Scextorders', 'Srintorders', 'Ssintorders', 'Externalservices',
                                           'Internalservices', 'Externaltransactions', 'Internaltransactions'],
                           edgeList=['Order', 'rcextship', 'scextship', 'srintship', 'ssintship', 'Related_To',
                                     'Manufactures', 'Orders_Prodcut', 'externaltransactions', 'internaltransactions'])
            print(self.allExistingGraphs)
        # if the graph exists, nothing happens
        else:
            print("Graph Already Exists")

    ## get all graphs names in the DB and save it in array (global variable)  return array
    # def getGraphs(self):
    #     # send the graph list command
    #     stat = "CALL gds.graph.list()"
    #     graphs = self.execute_Command(stat)
    #     temp = []
    #     # loop on the graphs and convert it to dictionary and add it to the array
    #     for graph in graphs:
    #         x = dict(graph)
    #         temp.append(x['graphName'])
    #     # return the array of all graphs exists in the database
    #     return temp


    def close(self):
        self.__driver.close()

    ## excute command function
    def execute_Command(self, command,write=False):
        # data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "123"))
        # data_base_connection = GraphDatabase.driver(uri="bolt://127.0.0.1:7687", auth=("neo4j", "123"))
        # session = data_base_connection.session()
            with self.__driver.session() as session:
                # output = session.run(command)
                if write:
                    output = session.execute_write(self.run_command_and_return_output,command)
                else:
                    output = session.execute_read(self.run_command_and_return_output,command)
                print("------------executed-----------------")
                return output
    ## get output of run:
    def run_command_and_return_output(self,tx,command):
        result = tx.run(command)
        return result
    ## delete the node "for future works"
    def deleteNode(self, label, id, nodeID):
        print("-----------Beginning --------------")
        print(label, id, nodeID)
        deleteNodeStatement = f"MATCH (a:{label} " + "{" + f"ID: {id}" + "}) DELETE a"
        print("------------Statment----------------")
        print(deleteNodeStatement)
        print("-----------Create DIC --------------")
        self.deletedNode = [{
            id: {
                'Label': label,
            }
        }]
        self.execute_Command(deleteNodeStatement)
        return self.deletedNode

    ## save graph and add its name in the array of graph names (the global variable)
    def saveGraph(self, name, nodeList, edgeList):
        saveStatment = '''
        CALL gds.graph.project(
        '%s',
        %s,
        %s,
        {
        relationshipProperties: 'weight'
        })
        YIELD
        graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipCount AS rels
        ''' % (name, nodeList, edgeList)
        print("----------------GRAPH SAVINGGGG----------------")
        self.execute_Command(saveStatment)
        self.allExistingGraphs.append(name)
        print("----------------GRAPH SAVED----------------")

    ## check if the graphname gived as an input already exists or not
    def ExistingGraph(self, name):
        for n in self.allExistingGraphs:
            if (n == name):
                return True
        return False

    def execute_transactions(self):
        from neo4j import GraphDatabase
        with self.__driver.session() as session:
            for command in self.__transaction_execution_commands:
                session.execute_write(lambda tx: tx.run(command))

            print("------------executed-----------------")




    def __add_delete_statement(self):
        delete_statement = "match (n) detach delete n"
        self.__transaction_execution_commands.append(delete_statement)

    def __add_nodes_statements(self):
        for node_index, node in self.nodes_df.iterrows():
            label = node["Label"].capitalize()
            IDs = node["ID"]
            attributes = node["Attributes"]
            create_statement = self.__node_create_statement(label, node_index, IDs, attributes)
            self.__transaction_execution_commands.append(create_statement)

    def __node_create_statement(self, label, index, IDs, attributes):
        att = (", " + self.__destructure_dict(attributes)) if len(attributes) > 0 else ""
        return f"CREATE (x:{label} {'{'}name: {self.__get_node_name(label, IDs)},index:{index},ID:{IDs} {att} {'}'})"

    def __get_node_name(self, label, id):
        node_name = '\"' + label.capitalize() + ' ' + str(id) + '\"'
        return node_name

    def __destructure_dict(self, attributes):
        import re
        attributes_string = ""
        for key in attributes:
            value = attributes[key]
            if isinstance(value, str):
                value = "\"" + value + "\""
            attributes_string = attributes_string + re.sub("[^\w_]", '', str(key).replace(" ", "_")) + ":" + str(
                value) + ","
        return attributes_string[:-1]

    def __add_edges_statements(self):
        for i, edge in self.edges_df.iterrows():
            create_relation_statement = self.__relation_create_statement(edge)
            self.__transaction_execution_commands.append(create_relation_statement)

    def __relation_create_statement(self, edge):
        from_id = edge['From']
        to_id = edge['To']
        from_name = edge['From_Table']
        to_name = edge['To_Table']
        rel_name = edge['Edge_Name']
        weight = edge['Weight']
        Transportation_Cost = edge['Transportation_Cost']
        Transportation_Distance = edge['Transportation_Distance']
        Transportation_Duration = edge['Transportation_Duration']
        Transportation_Type = edge['Transportation_Type']
        Rental_price = edge['Rental price']
        price = edge['price']
        profit_margin = edge['profit_margin (%)']
        market_share = edge['market_share (%)']
        Annual_sales = edge['Annual_sales']
        match_statement = f"Match (a:{from_name}),(b:{to_name}) WHERE a.index ={from_id} AND b.index = {to_id} "
        create_statement = "CREATE (a) - [r:%s { weight: %f , Transportation_Cost: %f , Transportation_Distance: %f , Transportation_Duration: %f , Transportation_Type: '%s' , Rental_price: %i, product_price: %f , profit_margin: %f , market_share: %i, Annual_sales:%f }]->(b)" % (
        rel_name, weight, Transportation_Cost, Transportation_Distance, Transportation_Duration, Transportation_Type,
        Rental_price, price, profit_margin, market_share, Annual_sales)
        print("-------statment---------")
        print(create_statement)
        create_relation_statement = match_statement + create_statement
        return create_relation_statement
