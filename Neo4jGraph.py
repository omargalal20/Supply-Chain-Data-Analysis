# from asyncio.windows_events import NULL
from neo4j import GraphDatabase
class Neo4jGraph:

    def __init__(self, nodes_df, edges_df):

        self.__DBusername = "neo4j"
        self.__DBpassword = "password"
        self.__DBuri = "bolt://localhost:7687"

        self.nodes_df = nodes_df
        self.edges_df = edges_df

        self.__transaction_execution_commands = []

        self.deleteSpecificNode = {}

        self.allExistingGraphs = self.getGraphs()

    ## draw and save graph if graph name doesn't exist in the database else print error
    def draw_graph(self,name):
        if(self.ExistingGraph(name)==False):
            self.__transaction_execution_commands = []
            self.__add_delete_statement()
            self.__add_nodes_statements()
            self.__add_edges_statemnts()
            self.execute_transactions()
            self.saveGraph(name,nodeList=['Customer','Products','Retailer','Supplier','Rcextship','Scextship','Srintship','Ssintship','Facilities','Warehouses','Rcextorders',
            'Scextorders','Srintorders','Ssintorders','Externalservices','Internalservices','Externaltransactions','Internaltransactions'],
            edgeList=['Order','rcextship','scextship','srintship','ssintship','Related_To',
            'Manufactures','Orders_Prodcut','externaltransactions','internaltransactions'])
        else:
            print("Graph Already Exists")
                   

    ## get all graphs names in the DB and save it in array (global variable)  return array        
    def getGraphs(self):
        stat = "CALL gds.graph.list()"
        graphs = self.execute_Command(stat)
        temp = []
        for graph in graphs:
            x = dict(graph)
            temp.append(x['graphName'])
        return temp

      ## excute command function
    def execute_Command(self,command):
        data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "password"))
        session = data_base_connection.session()
        output = session.run(command)
        print("------------executed-----------------")
        return output

    ## delete the node "for future works"
    def deleteNode(self,label,id,nodeID):
        print("-----------Beginning --------------")
        print(label,id,nodeID)
        deleteNodeStatement = f"MATCH (a:{label} " + "{"+ f"ID: {id}" + "}) DELETE a" 
        print("------------Statment----------------")
        print(deleteNodeStatement)
        print("-----------Create DIC --------------")
        self.deletedNode = [{
            id:{
                'Label':label,
            }
        }]
        self.execute_Command(deleteNodeStatement)
        return self.deletedNode
    
    ## save graph and add its name in the array of graph names (the global variable)
    def saveGraph(self,name,nodeList,edgeList):
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
        ''' %(name,nodeList,edgeList)
        print("-------statment---------")
        print(saveStatment)
        print("----------------GRAPH SAVINGGGG----------------")
        self.execute_Command(saveStatment)
        self.allExistingGraphs.append(name)
        print("----------------GRAPH SAVED----------------")
        
    ## check if the graphname gived as an input already exists or not
    def ExistingGraph(self,name):
        for n in self.allExistingGraphs:
            if(n == name):
                return True
        return False
            



    def execute_transactions(self):
        from neo4j import GraphDatabase
        # data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "123"))
        data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "password"))
        session = data_base_connection.session()
        for command in self.__transaction_execution_commands:
            session.run(command)

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

    def __add_edges_statemnts(self):
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
        match_statement = f"Match (a:{from_name}),(b:{to_name}) WHERE a.index ={from_id} AND b.index = {to_id} "
        create_statement = f"CREATE (a) - [r:{rel_name} {'{ weight: ' + str(weight) + ' }'}]->(b)"
        create_relation_statement = match_statement + create_statement
        return create_relation_statement
