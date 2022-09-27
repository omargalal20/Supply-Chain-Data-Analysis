import pandas as pd


class CriticalNodeTask:

    def __init__(self,myGraph,graphAnalysis,nodeNames,edgesNames,nodesTable,relationNames):
        self.myGraph = myGraph
        self.graphAnalysis = graphAnalysis
        self.nodeNames = nodeNames
        self.edgesNames = edgesNames
        self.nodesTable = nodesTable
        self.relationNames = relationNames

    # get the nodes and count of connected Nodes to it
    def getNodesWiththeCountsOfConnectedNodes(self,graphName,orientation=""):
        executedStatment = ""
        # check if there is orientation give or not
        if orientation == "":
            # if not, send the below command to neo4ji
            executedStatment = '''CALL gds.degree.stream('%s')
                                    YIELD nodeId, score
                                    RETURN gds.util.asNode(nodeId).name AS name, score AS followers
                                    ORDER BY followers DESC, name DESC
                                    '''%(graphName)
        
        else:
            # if so, send the command with orientation to neo4ji
           executedStatment = '''CALL gds.degree.stream('%s',{orientation:'%s'})
                                YIELD nodeId, score
                                RETURN gds.util.asNode(nodeId).name AS name, score AS followers
                                ORDER BY followers DESC, name DESC
                                '''%(graphName,orientation)
        # send the executed statment to neo4ji
        criticalNodes = self.myGraph.execute_Command(executedStatment)
        # convert the output to dataframe
        criticalNodesDF = self.returnCriticalNodesDF(criticalNodes)
        # validate the number of connected nodes
        criticalNodesDF = self.ValidatethecountsOfNodes(criticalNodesDF,self.nodeNames)
        criticalNodesDF.to_csv("critic.csv")
        return criticalNodesDF

    # takes the criticalNodes as neo4ji outout and return it as dataframe
    def returnCriticalNodesDF(self,criticalNodes):
        criticalNodesDF = pd.DataFrame()
        i = 0
        # loop over the output of neo4ji
        for criticalNode in criticalNodes:
            # convert it to dataframe
            x = dict(criticalNode)
            nodeName = x['name']
            nodeFollowers = x['followers']
            temp = pd.DataFrame({'name':[nodeName],
                                'followers':[nodeFollowers]})
            criticalNodesDF = pd.concat([criticalNodesDF,temp],ignore_index=True)
            i+=1
        # return the dataframe of nodes with the count of its connected nodes
        return criticalNodesDF

    # takes the criticalNodesDF and nodeNames and return the valid Nodes
    ## removes the nodes with zeros connected nodes and the nodes that are actual nodes
    def ValidatethecountsOfNodes(self,criticalNodesDF,nodeNames):
        temp = criticalNodesDF
        # loop over the criticalNodes dataFrame
        for node in range(len(temp)):
            # get name of the current node
            nodeLabel = (((criticalNodesDF.loc[node]['name']).split(" "))[0]).lower()
            # get followers of the current node
            nodeFollowers = criticalNodesDF.loc[node]['followers']
            # check if the current not in not in nodeNames "Not an actual Node" or has zero followers (doesn't connected to any node) or 
            # nodeLabel is customer
            ## if so, it should be removed
            if (nodeLabel not in nodeNames) | (nodeFollowers == 0) | (nodeLabel == 'customer'):
                criticalNodesDF = criticalNodesDF.drop(node)
        # reindex the dataframe and return it
        return criticalNodesDF.reset_index(drop=True) 
    
    #getcritical nodes respect to how many connected nodes coneected to it and returns list with their names
    def criticalNodesRespectToConnetedNodes(self,criticalNodesDF):
        # get followers
        followersColumn = criticalNodesDF['followers']
        # get the max followers 
        max = followersColumn.max()
        # apply this formula to get the threshold
        threshold = max - 10
        criticalNodesWithHightestConnectedNodes = []
        temp = criticalNodesDF
        # loop over the dataframe
        for node in range(len(temp)):
            # check if the node followers below the threshold remove it from the dataframe
            if criticalNodesDF.loc[node]['followers'] < threshold:
                criticalNodesDF = criticalNodesDF.drop(node)
        # reindex the dataframe
        criticalNodeDF = criticalNodesDF.reset_index(drop=True) 
        # add the nodes names to the list
        criticalNodesWithHightestConnectedNodes = criticalNodeDF['name'].to_list()
        # return the list
        return criticalNodesWithHightestConnectedNodes

    # use criticalNodeDF global variable --- ## return list of critical nodes that I can reach
    def ifIReachCriticalNode(self,sourceNodeName,graphName,crticalNodes):
        # organizing attributes
        findAllPathsDic = {'targetNodeName': "" , 'cases': 0, 'graphName': graphName,'relationship':"",'k':1,'TargetType':""}
        validatPathsDic = {'nodesNames':self.nodeNames,'edgesNames':self.edgesNames,'nodesTable':self.nodeNames,"desiredType":""}
        # get all paths 
        allPaths = self.graphAnalysis.mainMethod(sourceNodeName,True,findAllPathsDic,validatPathsDic)
        reachableCriticalNodes = []
        # loop ver all paths
        for path in range(len(allPaths)):
            # get node names of the current path
            nodeNamesOfCpath = (allPaths).loc[path]['nodeNames'] 
            # loop over critical nodes 
            for cNode in range(len(crticalNodes)):
                # get name of critical node
                criticalNode = (crticalNodes).loc[cNode]['name'] 
                # check if the critical node exist in the path from source
                if criticalNode in nodeNamesOfCpath:
                    # check if the critical node doesn't exist in the reachablecriticalnodes
                    if criticalNode not in reachableCriticalNodes:
                        # if not apend it to reachableCriticalNodes
                        reachableCriticalNodes.append(criticalNode)
        # if the reachablecriticalnodes is empty ---- so the source node isn't connected to any critical node
        if reachableCriticalNodes == []:
            print("The sourceNode can't reach any criticalNodes")
        return reachableCriticalNodes 

    # get critical Nodes that has unique location ---- takes the nodesTable and returns array with nodes that has unique locations
    ## node is string that is  represented as "Label+ID"    
    def getCriticalNodesRespectToLocation(self,nodesTable):
        # filter nodes table based on specific criteria (suppliers,retailers,warehouses) 
        nodes = (nodesTable[(nodesTable.Label == 'supplier')|(nodesTable.Label == 'retailer')|(nodesTable.Label == 'warehouses')]).reset_index(drop=True) 
        # get the unique locations
        uniqueLocations = self.getUniqueLocations((nodes.reset_index(drop=True)))
        # variables for future usages
        nodesWithUniqueLocations = []
        sourceNodeName = ""
        # loop over the table that has only suppliers,retailer,and warehouses
        for node in range(len(nodes)):
            # get location of the current node
            location = self.getLocationFromAttributes(nodes,node) 
            # check if the location of the current node is unique or not
            if location in uniqueLocations:
                # if it is unique, the current node should added in the list
                sourceNodeName = ((nodes.loc[node]['Label']).capitalize()) + str((nodes.loc[node]['ID']))
                nodesWithUniqueLocations.append(sourceNodeName)
        # the returned list contains nodes that has unique locations
        return nodesWithUniqueLocations
    
    # get the unique locations that occur in the nodesTable which have only suppliers,retailer,and warehouses
    def getUniqueLocations(self,filteredNodesTable):
        locationsList = []
        # loop over the table
        for node in range(len(filteredNodesTable)):
            # get the location of the current node
            location = self.getLocationFromAttributes(filteredNodesTable,node)
            # add it to array for future usages
            locationsList.append(location)
        # add the locations to a dataframe to use count method to count the occurence of each location
        locationsDataFrame = pd.DataFrame({'locations': locationsList})
        # count the occurence of each location 
        locationCounts = locationsDataFrame['locations'].value_counts()
        # filter the dataframe to have only locations that is unique (occur only one time in the nodesTable)
        locationCounts = locationCounts[locationCounts == 1]
        ## locationCounts returns series ... so we should convert it to dataframe in order to get the locations
        # convert the series to dataframe 
        locationCountsDF = locationCounts.to_frame()
        # get the index of the dataframe "the index has the unique locations" and convert it to list
        return (locationCountsDF.index).to_list()
    
    # get the location from attributes ... takes the table and the index of the node
    def getLocationFromAttributes(self,tableWhereIgetLocationFrom,node):
        ## the location attribute occurs in attributes list but not in the index for all nodes 
        # if the node is retailer or warehouses, so the location will be in index 0 of attributes list
        if (tableWhereIgetLocationFrom.loc[node]['Label'] == 'retailer') | (tableWhereIgetLocationFrom.loc[node]['Label'] == 'warehouses'):
            location = (tableWhereIgetLocationFrom.loc[node]['Attributes'])[0]
        # if the node is supplier, so the location will be in index 1 of attributes list
        else:
            location = (tableWhereIgetLocationFrom.loc[node]['Attributes'])[1]
        # return the location
        return location


    def criticalNodesRespectToPrices(self,edgesDF,sourceNodeName,graphName):
        findAllPathsDic = {'targetNodeName': "Retailer 25889" , 'cases': 1, 'graphName': graphName,'relationship':"",'k':7,'TargetType':""}
        validatPathsDic = {'nodesNames':self.nodeNames,'edgesNames':self.edgesNames,'nodesTable':self.nodeNames,"desiredType":""}
   
    # Supplier,Retailer,Warehouse
    def criticalNodesRespectToProduct(self,sourceLabel,nodesTable,edgesDF,nodes_df):
        # I have all the supplier
        filteredEdgesDF = (edgesDF[(edgesDF.From_Table == sourceLabel)&(edgesDF.To_Table == "Products")]).reset_index(drop=True) 
        filteredEdgesDF.to_csv("hello.csv")
        # I need to access supplier to -----
        criticalSupplierWithRespectToProduct = []
        repeatedProductsIndices = []
        trail = []
        for node in range(len(filteredEdgesDF)):
            if filteredEdgesDF.loc[node]['To'] in repeatedProductsIndices:
                continue
            To_Index = filteredEdgesDF.loc[node]['To'] 
            temp = (filteredEdgesDF[(filteredEdgesDF.To == To_Index)])
           
            if len(temp) == 1:
                sourceID = nodesTable.loc[(filteredEdgesDF.loc[node]['From'])]['ID']
                sourceNodeName = "Supplier " + str(sourceID)
                criticalSupplierWithRespectToProduct.append(sourceNodeName)
                print("to_index")
                print(To_Index)
                print(nodes_df.loc[To_Index]['Label'] + " " +str(nodes_df.loc[To_Index]['ID']))
            else:
                repeatedProductsIndices.append(filteredEdgesDF.loc[node]['To'])
        # print(filteredEdgesDF.to_csv("temppp.csv"))
        # print(criticalSupplierWithRespectToProduct)   
        # print(len(criticalSupplierWithRespectToProduct)) 
        
        #print(repeatedProductsIndices)








        

