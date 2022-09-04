# from asyncio.windows_events import NULL
import imp
from math import nan
from platform import node
from time import sleep
import neo4j
import pandas as pd

class Neo4jGraph:

    def __init__(self, nodes_df, edges_df):

        self.__DBusername = "neo4j"
        self.__DBpassword = "password"
        self.__DBuri = "bolt://localhost:7687"

        self.nodes_df = nodes_df
        self.edges_df = edges_df

        self.__transaction_execution_commands = []

        self.deleteSpecificNode = {}

    def draw_graph(self,name):
        self.__transaction_execution_commands = []
        self.__add_delete_statement()
        self.__add_nodes_statements()
        self.__add_edges_statemnts()
        self.execute_transactions()
        self.saveGraph(name,nodeList=['Customer','Products','Retailer','Supplier','Rcextship','Scextship','Srintship','Ssintship','Facilities','Warehouses','Rcextorders',
        'Scextorders','Srintorders','Ssintorders','Externalservices','Internalservices','Externaltransactions','Internaltransactions'],
        edgeList=['Order','rcextship','scextship','srintship','ssintship','Related_To',
        'Manufactures','Orders_Prodcut','externaltransactions','internaltransactions'])


    def trail(self,label,id,nodeID):
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
    
    def findAllPaths(self,sourceNodeName,label,cases,graphName,relationShip="",k=1,targetNodeName=''):
        executedStatment =""
        print("beginning")
        if(cases == 0):
            print("All paths with no target")
            executedStatment = '''
            MATCH (source:%s {name:'%s'} )
            CALL gds.allShortestPaths.dijkstra.stream('%s', {
            sourceNode: source,
            relationshipWeightProperty: 'weight',
            relationshipTypes: ['%s']})
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
            index,
            gds.util.asNode(sourceNode).name AS sourceNodeName,
            gds.util.asNode(targetNode).name AS targetNodeName,
            totalCost,
            [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
            costs,
            nodes(path) as path
            ORDER BY index
            ''' % (label,sourceNodeName,graphName,relationShip )
        elif (cases == 1):
            print("morethan one path with target")
            executedStatment = '''
            MATCH (source:%s {name: '%s'}), (target:%s {name: '%s'})
            CALL gds.shortestPath.yens.stream('%s',{sourceNode:source, targetNode:target, k:%s , relationshipWeightProperty:'weight',
            relationshipTypes:['%s']})
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
                index,
                gds.util.asNode(sourceNode).name AS sourceNodeName,
                gds.util.asNode(targetNode).name AS targetNodeName,
                totalCost,
                [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
                costs,
                nodes(path) as path
            ORDER BY index
            '''% (label,sourceNodeName,label,targetNodeName,graphName,k,relationShip)
        elif (cases == 2):
            print("only one path")
            executedStatment = '''
            MATCH (source:%s {name: '%s'}), (target:%s {name: '%s'})
            CALL gds.shortestPath.dijkstra.stream('%s', {
            sourceNode: source,
            targetNode: target,
            relationshipWeightProperty: 'weight'
            })
            YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs, path
            RETURN
            index,
            gds.util.asNode(sourceNode).name AS sourceNodeName,
            gds.util.asNode(targetNode).name AS targetNodeName,
            totalCost,
            [nodeId IN nodeIds | gds.util.asNode(nodeId).name] AS nodeNames,
            costs,
            nodes(path) as path
            ORDER BY index
            ''' % (label,sourceNodeName,label,targetNodeName,graphName)
        print("----------------CASE EXCUTION----------------")
        output = self.execute_Command(executedStatment,True)
        print(output)
        print("-------------------done--------------")
    def saveGraph(self,name,nodeList,edgeList):
        print("beginning")
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
        self.execute_Command(saveStatment,False)
        print("----------------GRAPH SAVED----------------")
   

    def execute_transactions(self):
        from neo4j import GraphDatabase
        data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "password"))
        session = data_base_connection.session()
        for command in self.__transaction_execution_commands:
            session.run(command)

    def execute_Command(self,command,whichCommand):
        from neo4j import GraphDatabase
        data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "password"))
        session = data_base_connection.session()
        output = session.run(command)
        if(whichCommand):
            for i in output:
                print(pd.DataFrame(dict(i)))
            print("----------DataFrame------------")
            self.returnPaths(output)
        print("------------executed-----------------")
        return output

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

    def returnPaths (self,output):
        nodeNames = []
        costs = []
        curIndex = 0
        outputIndex = 0
        totalCosts = 0
        sourceNodeName = ""
        targetNodeName = ""
        df = pd.DataFrame()
        print("before for loop")
        count = 0
        for j in output:
              k = pd.DataFrame(dict(j))
              count += self.countSameIndex(k,0)
              print(count)
              print("I'm inside the first for loop")
        for i in output:
            x = pd.DataFrame(dict(i))

            print("before the if")
            if (x.loc[curIndex,"index"] == outputIndex) | (x.loc[curIndex+1,"index"] != None):
                costs.append(x.loc[curIndex,"costs"])
                nodeNames.append(x.loc[curIndex,"nodeNames"])
                print("I'm inside the if")
            else:
                x.loc[curIndex,"costs"] = costs
                x.loc[curIndex,"nodeNames"] = nodeNames
                df = pd.concat([df, x], ignore_index=True)
                outputIndex = outputIndex + 1
                print("helloo")
            curIndex = curIndex + 1
        # print(df)

    def countSameIndex(output, index):
        print("inside counting method")
        count = 0
        for i in range(len(output)):
            if(output.loc[i,"index"] == index):
                count += 1
        print(count)