import pandas as pd

class GraphAnalysis:


    def __init__(self,myGraph,nodesTable,edgesTable):
        self.myGraph = myGraph
        self.pathsWithCorrectTargetNodes = set()
        self.addColumnToRetailer(nodesTable,edgesTable)

    ## find all paths depending on the case 
    def findAllPaths(self,sourceNodeName,sourceLabel,cases,graphName,relationShip="",k=1,targetNodeName='',targetLabel=''):
        ## In all the cases, the relationshup is optional
        ## In all the cases, 
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
        dataFrameOutPut = self.execute_Command(executedStatment)
        print("-------------------done--------------")
        return dataFrameOutPut

    # find all paths viseVerse
    def findAllPathsViseVerse(self,SourceLabel,SourceNodeName,TargetLabel,nodesTable,graphName,nodeNames,edgesNames,TargetType=""):
        ### Retailer ---> supplier //Done
        ### Customer ---> Retailer
        ### Target --- supplier && source ---- Retailer
        ### Target --- Retailer
        outputtt = pd.DataFrame()

        # filter the nodesTable based on the target we want to reach
        out = nodesTable[(nodesTable.Label == (TargetLabel.lower()))].reset_index(drop=True) 
        # filter the out based on the type 
        filteredNodesTable = self.filterType(out,TargetType)
        # loop over the dataframe that contains the filtered data "destired target + the type"
        for node in range(len(filteredNodesTable)):
            souceNode = TargetLabel+ " " + str(filteredNodesTable.loc[node]['ID'])
            out = self.findAllPaths(sourceNodeName=souceNode,sourceLabel=TargetLabel,cases=2,graphName=graphName,targetNodeName=SourceNodeName,targetLabel=SourceLabel)
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
        from neo4j import GraphDatabase
        data_base_connection = GraphDatabase.driver(uri=self.myGraph.DBuri, auth=(self.myGraph.DBusername, self.myGraph.DBpassword))
        session = data_base_connection.session()
        # sends the output to neo4ji
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
    def validatePath(self,paths,sourceNodeName,nodeNames,edgesNames):
        # filter Target nodes
        # delete the unvalid rows with unvalid target nodes
        self.targetNodeValidation(paths,sourceNodeName,nodeNames)
        # Take valid nodes names from the valid rows
        # validate these rows && classify if they are direct or not
        finalPaths = self.pathsValidation(self.pathsWithCorrectTargetNodes,nodeNames,edgesNames)
        #finalPaths.to_csv("try.csv")
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
        self.pathsWithCorrectTargetNodes =  dataFramePaths.reset_index(drop=True) 
        return self.pathsWithCorrectTargetNodes
    
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
            ### note
            if(len(nodeNamesColumn[path]) == 1):
                dataFramePaths = dataFramePaths.drop(path)
                continue
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

    ## check if the type of the connected direct nodes matches or not
    def lastCheckOnPath(self,dataFrameOfPaths,nodesTable,theDesiredType=''):
        FinalPaths = dataFrameOfPaths
        isDirect = ""
        print(range(len(dataFrameOfPaths)))
        for path in range(len(dataFrameOfPaths)):
            if(dataFrameOfPaths.loc[path]["isDirect"] == True):
                isDirect = dataFrameOfPaths.loc[path]['sourceNodeName']
            else:
                pathNodeNames = dataFrameOfPaths.loc[path]['nodeNames']
                isDirect = pathNodeNames[-3]
            
            nodeType = self.getType(isDirect,nodesTable)
            if(type(nodeType) == set):
                if(theDesiredType not in nodeType):
                    if len(pathNodeNames)>=5:
                        nodeType = self.getType(pathNodeNames[-5],nodesTable)
                        if((theDesiredType not in nodeType)):
                            FinalPaths = FinalPaths.drop(path)
            else:
                 if(nodeType != theDesiredType):
                    FinalPaths = FinalPaths.drop(path)

        return FinalPaths.reset_index(drop=True) 

    def getType(self,sourceNodeName,nodesTable):
        NodeLabel = ""
        NodeID = 0
        arrayOfNodeName =sourceNodeName.split(" ")
        NodeLabel = arrayOfNodeName[0].lower()
        NodeID = int(arrayOfNodeName[1])
        nodeItSelf= nodesTable[(nodesTable.Label == NodeLabel) & (nodesTable.ID == NodeID)]
        nodeType = ((nodeItSelf.Attributes).iloc[0])[4]
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



  
                
                
                        

        


        



                    

                

            







        

                        




