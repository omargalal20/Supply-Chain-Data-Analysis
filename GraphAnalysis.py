from mimetypes import init
from re import X
import pandas as pd
import neo4j
from Neo4jGraph import Neo4jGraph

class GraphAnalysis:


    def __init__(self,nodes_df,edges_df):
        self.myGraph = Neo4jGraph(nodes_df,edges_df)
    
  

    ## find all paths are return the dataframe
    def findAllPaths(self,sourceNodeName,sourceLabel,cases,graphName,relationShip="",k=1,targetNodeName='',targetLabel=''):
        if(self.myGraph.ExistingGraph(graphName) == False):
            print("Graph doesn't exist in the database")
            return
        executedStatment =""
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
            ''' % (sourceLabel,sourceNodeName,graphName,relationShip )
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
            '''% (sourceLabel,sourceNodeName,targetLabel,targetNodeName,graphName,k,relationShip)
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
            ''' % (sourceLabel,sourceNodeName,targetLabel,targetNodeName,graphName)
        print("----------------CASE EXCUTION----------------")
        dataFrameOutPut = self.execute_Command(executedStatment)
        print("-------------------done--------------")
        return dataFrameOutPut


    def execute_Command(self,command):
        from neo4j import GraphDatabase
        data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "123"))
        session = data_base_connection.session()
        output = session.run(command)
        output =  self.returnPaths(output)
        print("------------executed-----------------")
        return output

    ## Return paths as dataFrame
    def returnPaths (self,output):
        temp = pd.DataFrame()
        final = pd.DataFrame()
        for i in output:
            x = dict(i)
            index = x["index"]
            sourceNodeName = x["sourceNodeName"]
            targetNodeName = x["targetNodeName"]
            totalCost = x["totalCost"]
            nodeNames = x["nodeNames"]
            costs = x["costs"]
            tmp = pd.DataFrame({'index': [index],
                                'sourceNodeName': [sourceNodeName],
                                'targetNodeName': [targetNodeName],
                                'totalCost': [totalCost]})

            costs_df = pd.DataFrame({'costs': [costs]})

            result = pd.merge(
                 tmp,
                 costs_df,
                 how='left',
                 left_index=True, 
                 right_index=True 
                 )
          
            nodeNames_df = pd.DataFrame({'nodeNames': [nodeNames]})
            temp = pd.merge(
                result,
                nodeNames_df,
                how='left',
                left_index=True, 
                right_index=True 
            )
            final = pd.concat([final,temp], ignore_index=True)
        return final