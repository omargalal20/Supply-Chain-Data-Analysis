from queue import Empty
import pandas as pd

class GraphAnalysis:


    def __init__(self,myGraph,nodesTable,edgesTable):
        self.myGraph = myGraph
        self.pathsWithCorrectTargetNodes = set()
        self.addColumnToRetailer(nodesTable,edgesTable)
    # takes the attributes below and returns the final dataframe which has the validated paths
        ## sourceNodeName --- represents source node
        ## whichMethod --- if true, findAllPaths used; otherwise, findAllPathsViseVerse used
        ## findAllPathsSet -- has the attributes used to as inputs in findpaths
        ## validaPathsDic -- has attributes used as inputs in validatepaths
    def mainMethod(self,sourceNodeName,whichMethod,findAllPathsDic,validaPathsDic):
        cases=findAllPathsDic['cases']
        graphName=findAllPathsDic['graphName']
        relationship = findAllPathsDic['relationship']
        k = findAllPathsDic['k']
        targetNodeName = findAllPathsDic['targetNodeName']
        targetType = findAllPathsDic['TargetType']
        nodesTable = validaPathsDic['nodesTable']
        nodesName = validaPathsDic['nodesNames']
        edgesName = validaPathsDic['edgesNames']
        thedesiredType = validaPathsDic['desiredType']
        if(whichMethod):
            paths = self.findAllPaths(sourceNodeName=sourceNodeName,cases=cases,graphName=graphName,relationShip=relationship,k=k,targetNodeName=targetNodeName)
        else:
            paths = self.findAllPathsViseVerse(sourceNodeName=sourceNodeName,nodesTable=nodesTable,graphName=graphName,nodeNames=nodesName,edgesNames=edgesName,k=k,TargetType=targetType)
        if paths.empty :
            print("there are no paths to be validated")
            return
        finalOutput = self.validatePath(paths=paths,sourceNodeName=sourceNodeName,nodeNames=nodesName,edgesNames=edgesName,nodesTable=nodesTable,theDesiredType=thedesiredType)
        
        return finalOutput
    
    ## find all paths depending on the case 
    def findAllPaths(self,sourceNodeName,cases,graphName,relationShip="",k=1,targetNodeName=""):
        ## In all the cases, the relationshup is optional
        ## In all the cases, 
        sourceLabel = (sourceNodeName.split(" "))[0]
        targetLabel = "" if targetNodeName == "" else (targetNodeName.split(" "))[0]

        # check if the graph already exists in the database or not
        if(self.myGraph.ExistingGraph(graphName) == False):
            print("Graph doesn't exist in the database")
            return
        executedStatment ="" # the statment that will be sent to neo4ji
        # case 0 which runs dijkstra algorithm and returns all paths from the source to all reachable nodes
        if(cases == 0):
            # if the relationship is given
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
            # if the relationship isn't given
            else:
               
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
        # case 1 runs yen algorithm and returns atleast one path from the source to the target
        elif (cases == 1):
            # if the relationship is given
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
             # if the relationship isn't given
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
        # case 2 runs dijkstra algorithm and returns only one path from the source to the target
        elif (cases == 2):
            # if the relationship is given
            if(relationShip!=""):
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
            # if the relationship isn't given
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
        # execute the command and returns the dataframe with the paths returned from neo4ji
        # dataFrameOutPut = self.execute_Command(executedStatment)
        neo4j_output = self.myGraph.execute_Command(executedStatment)
        dataFrameOutPut = self.returnPaths(neo4j_output)
        print("-------------------done--------------")
        return dataFrameOutPut

    # find all paths viseVerse
    def findAllPathsViseVerse(self,SourceNodeName,nodesTable,graphName,nodeNames,edgesNames,cases,k=1,TargetType=""):
        ### Retailer ---> supplier //Done
        ### Customer ---> Retailer
        ### Target --- supplier && source ---- Retailer
        ### Target --- Retailer
        outputtt = pd.DataFrame()
        SourceLabel = (SourceNodeName.split(" "))[0]
        TargetLabel = (TargetLabel.split(" "))[0]
        # filter the nodesTable based on the target we want to reach
        out = nodesTable[(nodesTable.Label == (TargetLabel.lower()))].reset_index(drop=True) 
        # filter the out based on the type 
        filteredNodesTable = self.filterType(out,TargetType)
        # loop over the dataframe that contains the filtered data "destired target + the type"
        for node in range(len(filteredNodesTable)):
            souceNode = TargetLabel+ " " + str(filteredNodesTable.loc[node]['ID'])
            out = self.findAllPaths(sourceNodeName=souceNode,sourceLabel=TargetLabel,cases=cases,graphName=graphName,k=k,targetNodeName=SourceNodeName,targetLabel=SourceLabel)
            # if there is no paths between the source and the target
            if(out.empty):
                continue
            # validate the paths returned
            temp = self.validatePath(out,souceNode,nodeNames,edgesNames)
            outputtt = pd.concat([outputtt,temp], ignore_index=True)
        return outputtt
    
    # filter the filtered nodes table which has only the target nodes based on the type
    def filterType(self,filteredTable,desiredType):
        temp = filteredTable
        for node in range(len(filteredTable)):
            # if the node is retailer, its attributes will be a set
            if type(list(temp["Attributes"].loc[node])[4]) == set:
            ## if type(list(temp["Attributes"].loc[node])[4]) == list: "ahmed merge"
                # search if the desired type isn't in the set of attributes
                if desiredType not in (list(temp["Attributes"].loc[node])[4]):
                    # drop it
                    temp = temp.drop(node)
            # else if the node is a supplier, its attribute will be a string
            elif(list(temp["Attributes"].loc[node])[4] != desiredType):
                temp = temp.drop(node)
        # reset the index of the filtered dataframe
        return temp.reset_index(drop=True) 

    # takes the command and send it to neo4ji, converts neo4ji output to dataframe and returns it
    def execute_Command(self,command):
        data_base_connection = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "123"))
        #data_base_connection = GraphDatabase.driver(uri="bolt://127.0.0.1:7687", auth=("neo4j", "123"))
        #session = data_base_connection.session()
        # sends the output to neo4ji
        with data_base_connection.session() as session:
            output = session.run(command)
            # converts the output to dataframe
            output =  self.returnPaths(output)
            print("------------executed-----------------")
            return output

        
    ## Return paths as dataFrame
    def returnPaths (self,output):
        temp = pd.DataFrame()
        final = pd.DataFrame()
        # the output from neo4ji is of type <Result> 
        for i in output:
            # converts each path to dictionary
            x = dict(i)
            # get the columns from the dictionary and merge them
            index = x["index"]
            sourceNodeName = x["sourceNodeName"]
            targetNodeName = x["targetNodeName"]
            totalCost = x["totalCost"]
            nodeNames = x["nodeNames"]
            costs = x["costs"]
            ## in case of the array, in order to add the whole array in only one cell, we should add it to individual dataframe and merge it
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
    def validatePath(self,paths,sourceNodeName,nodeNames,edgesNames,nodesTable,theDesiredType):
        # filter Target nodes
        # delete the unvalid rows with unvalid target nodes
        pathsWithCorrectTargetNodes = self.targetNodeValidation(paths,sourceNodeName,nodeNames)
        # Takes valid nodes names from the valid rows
        # validate these rows && classify if they are direct or not
        secondPaths = self.pathsValidation(pathsWithCorrectTargetNodes,nodeNames,edgesNames)
        # Takes valid paths with valid target nodes
        # check on the types of the nodes in the path
        finalPaths = self.lastCheckOnPath(secondPaths,nodesTable,theDesiredType)

        return finalPaths
    
    ## Valid target nodes
    def targetNodeValidation(self,paths,sourceNodeName,nodeNames):
        # get all the target nodes
        targetNodesColumn =  paths["targetNodeName"]
        dataFramePaths = paths
        # loop over the paths
        for path in range(len(paths)):
            # check if the current target node == the source node and drop if it's true
            if(targetNodesColumn[path] == sourceNodeName):
                dataFramePaths = dataFramePaths.drop(path)
            # check if the target node is actual node not edge and drop if it's true
            else:
                targetNodeTemp = ((targetNodesColumn[path].split(" "))[0]).lower()
                if(targetNodeTemp not in nodeNames):
                    dataFramePaths = dataFramePaths.drop(path)
        # after dropping, we should reindex the table 
        pathsWithCorrectTargetNodes =  dataFramePaths.reset_index(drop=True) 
        return pathsWithCorrectTargetNodes
    
    ## Validate the paths of the valid target nodes
    def pathsValidation(self,pathsWithCorrectTargetNodes,nodeNames,edgesNames):
        # get the nodeNames, the path
        nodeNamesColumn =  pathsWithCorrectTargetNodes["nodeNames"]
        dataFramePaths = pathsWithCorrectTargetNodes
        previousNode = ""
        currentNode = ""
        validUntilNow = False
        # loop over the paths 
        for path in range(len(pathsWithCorrectTargetNodes)):
            # loop over the path and check if the path moving from node -> edge -> node -> edge
            for element in  range(len(nodeNamesColumn[path])-1):
                previousNode = (((nodeNamesColumn[path])[element]).split(" ")[0]).lower()
                currentNode = (((nodeNamesColumn[path])[element+1]).split(" ")[0]).lower()
                if ((previousNode in nodeNames) & (currentNode in edgesNames)) | ((previousNode in edgesNames) & (currentNode in nodeNames)):
                    validUntilNow = True
                else:
                    validUntilNow = False
                    break
            # if the path isn't valid, drop it
            if(validUntilNow == False):
                dataFramePaths = dataFramePaths.drop(path)
            # if the path is valid, edit the isDirect attribute in the nodes table   
            else:
                if(len(nodeNamesColumn[path]) == 3):
                    dataFramePaths.at[path,'isDirect'] = True
                ### special pathhh
                else:
                    dataFramePaths.at[path,'isDirect'] = False
        # reindex the dataFrame
        finalApprovedPaths = dataFramePaths.reset_index(drop=True) 
        return finalApprovedPaths

    ## check if the type of the connected nodes matches or not and return dataframe of valid paths
    def lastCheckOnPath(self,dataFrameOfPaths,nodesTable,theDesiredType=''):
        FinalPaths = dataFrameOfPaths
        isDirect = ""
        lengthOfPath = 3 ## in case of direct path
       ## loop over the paths
        for path in range(len(dataFrameOfPaths)):
            # if the path is direct
            if(dataFrameOfPaths.loc[path]["isDirect"] == True):
                # assign the variable with the source node name
                isDirect = dataFrameOfPaths.loc[path]['sourceNodeName']
            # if the path is indirect
            else:
                # assign the variables with the first previous connected to the target node
                pathNodeNames = dataFrameOfPaths.loc[path]['nodeNames']
                lengthOfPath = len(pathNodeNames) ## to check the length of the path (for future needs)
                isDirect = pathNodeNames[-3]

            # get type of the node (source "in case of direct path" or the previous node "in case of the indirect path")
            nodeType = self.getType(isDirect,nodesTable)
            
            # check if the node is retailer
            if(type(nodeType) == set):
        # if(type(nodeType) == list) after merging with ahmed
                # if the retailer has the same type of the desired
                if(theDesiredType in nodeType):
                    # check the type of first supplier connected to the retailer 
                    if lengthOfPath>=5:
                        nodeType = self.getType(pathNodeNames[-5],nodesTable)
                        # if the supplier doesn't have the same type of the desired node, the path should be dropped
                        if((theDesiredType != nodeType)):
                            FinalPaths = FinalPaths.drop(path)
                # if the retailer doesn't have the desired type
                else:
                    FinalPaths = FinalPaths.drop(path)
            # if the node isn't retailer 
            else:
                # if the node doesn't have the desired type
                if(nodeType != theDesiredType):
                    FinalPaths = FinalPaths.drop(path)
        # reindex the dataframe and return it
        return FinalPaths.reset_index(drop=True) 

    # get the type of the node and return it
    def getType(self,sourceNodeName,nodesTable):
        NodeLabel = ""
        NodeID = 0
        # split the name as label and id
        arrayOfNodeName =sourceNodeName.split(" ")
        NodeLabel = arrayOfNodeName[0].lower()
        NodeID = int(arrayOfNodeName[1])
        # filter the nodes table based on the label and id
        nodeItSelf= nodesTable[(nodesTable.Label == NodeLabel) & (nodesTable.ID == NodeID)]
        # get the type from the attributes
        nodeType = ((nodeItSelf.Attributes).iloc[0])[4]
        # return the type
        return nodeType    

    # add type attribute to retailer, the type based on the types of suppliers connected to the retailer
    def addColumnToRetailer(self,nodesTable,edgesTable):
        for node in range(len(nodesTable)):
            if(nodesTable.loc[node]['Label'] == 'retailer'):
                RetailerTypes = set()
                ### all nodes that is connected to the retailer with node as ID
                whatisConnectedToRetailer = edgesTable[(edgesTable.To_Node_ID == node)].reset_index(drop=True)
                if(whatisConnectedToRetailer.empty):
                    continue
                ## loop on the all node connected to retailer to get the suppliers connected to retailer
                for edge in range(len(whatisConnectedToRetailer)):
                    ## From_Node_ID 
                    idConnectedtoRetailer = whatisConnectedToRetailer.loc[edge]['From_Node_ID']
                    ## check if From_Node_ID is supplier
                    if nodesTable.loc[idConnectedtoRetailer]['Label'] == 'supplier':
                        ## list of attributes of supplier connected to the retailer
                        AttributesOfSupplier = list(nodesTable.loc[idConnectedtoRetailer]['Attributes'])
                        ## type of supplier that is connected to the retailer
                        typeOfSupplier = AttributesOfSupplier[4]
                        RetailerTypes.add(typeOfSupplier)
                if(len(RetailerTypes) != 0):
                    ## adding column
                    listOfAttributes = list(nodesTable.loc[node]['Attributes'])
                    listOfAttributes.insert(4,RetailerTypes)
                    nodesTable.loc[node]['Attributes'] = listOfAttributes    
       # nodesTable.to_csv("nodesTable.csv")



  
                
                
                        

        


        



                    

                

            







        

                        




