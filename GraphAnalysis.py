from mimetypes import init
from re import X
import pandas as pd
import neo4j
from Neo4jGraph import Neo4jGraph

class GraphAnalysis:


    def __init__(self,nodes_df,edges_df):
        self.myGraph = Neo4jGraph(nodes_df,edges_df)
        self.pathsWithCorrectTargetNodes = set()
    
  

    ## find all paths are return the dataframe
    def findAllPaths(self,sourceNodeName,sourceLabel,cases,graphName,relationShip="",k=1,targetNodeName='',targetLabel=''):
        if(self.myGraph.ExistingGraph(graphName) == False):
            print("Graph doesn't exist in the database")
            return
        executedStatment =""
        if(cases == 0):
            print("All paths with no target")
            if(relationShip!=""):
                print("All paths with no target and with relation")
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
            else:
                print("All paths with no target and with no relation")
                executedStatment = '''
                MATCH (source:%s {name:'%s'} )
                CALL gds.allShortestPaths.dijkstra.stream('%s', {
                sourceNode: source,
                relationshipWeightProperty: 'weight'})
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
                ''' % (sourceLabel,sourceNodeName,graphName)
        elif (cases == 1):
            print("morethan one path with target")
            if(relationShip!=""):
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
            else:
                executedStatment = '''
                MATCH (source:%s {name: '%s'}), (target:%s {name: '%s'})
                CALL gds.shortestPath.yens.stream('%s',{sourceNode:source, targetNode:target, k:%s , relationshipWeightProperty:'weight'})
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
                '''% (sourceLabel,sourceNodeName,targetLabel,targetNodeName,graphName,k)
        elif (cases == 2):
            print("only one path")
            if(relationShip!=""):
                print("only one path with relationship")
                executedStatment = '''
            MATCH (source:%s {name: '%s'}), (target:%s {name: '%s'})
            CALL gds.shortestPath.dijkstra.stream('%s', {
            sourceNode: source,
            targetNode: target,
            relationshipWeightProperty: 'weight',
            relationshipTypes:['%s']
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
            ''' % (sourceLabel,sourceNodeName,targetLabel,targetNodeName,graphName,relationShip)
            else:   
                print("only one path with no relationship")
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
                                'totalCost': [totalCost],
                                'isDirect':[None]})

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
    
    ## validate paths and return the final approved paths
    def validatePath(self,paths,sourceNodeName,nodeNames,edgesNames):
        # filter Target nodes
        # delete the unvalid rows with unvalid target nodes
        self.targetNodeValidation(paths,sourceNodeName,nodeNames)
        # Take valid nodes names from the valid rows
        # validate these rows && classify if they are direct or not
        finalPaths = self.pathsValidation(self.pathsWithCorrectTargetNodes,nodeNames,edgesNames)
        finalPaths.to_csv("try.csv")
        return finalPaths


    
    
    ## Valid target nodes
    def targetNodeValidation(self,paths,sourceNodeName,nodeNames):
        targetNodesColumn =  paths["targetNodeName"]
        dataFramePaths = paths
        for path in range(len(paths)):
            if(targetNodesColumn[path] == sourceNodeName):
                dataFramePaths = dataFramePaths.drop(path)
            else:
                targetNodeTemp = ((targetNodesColumn[path].split(" "))[0]).lower()
                if(targetNodeTemp not in nodeNames):
                    dataFramePaths = dataFramePaths.drop(path)
        self.pathsWithCorrectTargetNodes =  dataFramePaths.reset_index(drop=True) 
        return self.pathsWithCorrectTargetNodes
    
    ## Validate the paths of the valid target nodes

    def pathsValidation(self,pathsWithCorrectTargetNodes,nodeNames,edgesNames):
        nodeNamesColumn =  pathsWithCorrectTargetNodes["nodeNames"]
        dataFramePaths = pathsWithCorrectTargetNodes
        previousNode = ""
        currentNode = ""
        validUntilNow = False
        for path in range(len(pathsWithCorrectTargetNodes)):
            if(len(nodeNamesColumn[path]) == 1):
                dataFramePaths = dataFramePaths.drop(path)
                continue
            for element in  range(len(nodeNamesColumn[path])-1):
                previousNode = (((nodeNamesColumn[path])[element]).split(" ")[0]).lower()
                currentNode = (((nodeNamesColumn[path])[element+1]).split(" ")[0]).lower()
                if ((previousNode in nodeNames) & (currentNode in edgesNames)) | ((previousNode in edgesNames) & (currentNode in nodeNames)):
                    validUntilNow = True
                else:
                    validUntilNow = False
                    break
            if(validUntilNow == False):
                dataFramePaths = dataFramePaths.drop(path)
            else:
                if(len(nodeNamesColumn[path]) == 3):
                    dataFramePaths.at[path,'isDirect'] = True
                else:
                    dataFramePaths.at[path,'isDirect'] = False
        finalApprovedPaths = dataFramePaths.reset_index(drop=True) 
        return finalApprovedPaths

    ## check if the type of the connected direct nodes matches or not
    def lastCheckOnPath(self,dataFrameOfPaths,nodeTables,theDesiredType=''):
        NodeLabel = ""
        NodeID = 0
        FinalPaths = dataFrameOfPaths
        for path in range(len(dataFrameOfPaths)):
            if(dataFrameOfPaths.loc[path]["isDirect"] == True):
                ## source node elly fel table
                sourceNodeName = dataFrameOfPaths.loc[path]['sourceNodeName'].split(" ")
                NodeLabel = sourceNodeName[0].lower()
                NodeID = int(sourceNodeName[1])
                ## get attributes of source node
                x = nodeTables[(nodeTables.Label == NodeLabel) & (nodeTables.ID == NodeID)]
                nodeType = ((x.Attributes).iloc[0])[4]
                ## check on type
                if(nodeType != theDesiredType):
                    FinalPaths = FinalPaths.drop(path)
            else:
                pathNodeNames = dataFrameOfPaths.loc[path]['nodeNames']
                ###  print(type(pathNodeNames)) tl3t list
                ### supplier ---- Customer "Indirect
        FinalPaths = FinalPaths.reset_index(drop=True) 
        FinalPaths.to_csv("final4.csv")
                    

                

            







        

                        




