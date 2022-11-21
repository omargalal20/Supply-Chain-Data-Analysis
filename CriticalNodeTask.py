import ast
from random import randint
import pandas as pd
from GraphAnalysis import GraphAnalysis
# from scipy.integrate import quad
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px


class CriticalNodeTask:

    def __init__(self,myGraph,graphAnalysis,nodeNames,edgesNames,nodesTable,relationNames):
        self.myGraph = myGraph
        self.graphAnalysis = graphAnalysis
        self.nodeNames = nodeNames
        self.edgesNames = edgesNames
        self.nodesTable = nodesTable
        self.relationNames = relationNames


    # get the nodes and count of connected Nodes to it (det degree of each node)
    def getNodesWiththeCountsOfConnectedNodes(self,graphName,orientation=""):
        executedStatment = ""
        # check if there is orientation give or not
        if orientation == "":
            # if not, send the below command to neo4ji --- orientation natural (default)
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

        suppliersCNDF =  (criticalNodesDF[criticalNodesDF['name'].str.contains("Supplier")]).reset_index(drop=True) 
        retailerCNDF = (criticalNodesDF[criticalNodesDF['name'].str.contains("Retailer")]).reset_index(drop=True) 
        warehousesCNDF = (criticalNodesDF[criticalNodesDF['name'].str.contains("Warehouses")]).reset_index(drop=True) 

        suppliersCNDF = suppliersCNDF[suppliersCNDF.followers>0].reset_index(drop=True) 
        retailerCNDF = retailerCNDF[retailerCNDF.followers>0].reset_index(drop=True) 
        warehousesCNDF = warehousesCNDF[warehousesCNDF.followers>0].reset_index(drop=True) 
            
        return suppliersCNDF,retailerCNDF,warehousesCNDF

    
    def getActualFollowersToNode(self,graphName):

        suppliersCNDF,retailerCNDF,warehousesCNDF = self.getNodesWiththeCountsOfConnectedNodes(graphName,"UNDIRECTED")

        for supplier in range(len(suppliersCNDF)):
            followersDeleted = self.validateConnectedNodes(suppliersCNDF.loc[supplier]["name"])
            actualFollowers = (suppliersCNDF.loc[supplier]["followers"]) - followersDeleted
            suppliersCNDF.at[supplier, 'followers'] = actualFollowers
        
        for retailer in range(len(retailerCNDF)):
            followersDeleted = self.validateConnectedNodes(retailerCNDF.loc[retailer]["name"])
            actualFollowers = (retailerCNDF.loc[retailer]["followers"]) - followersDeleted
            retailerCNDF.at[retailer, 'followers'] = actualFollowers
        
        suppliersCNDF = (self.ValidatethecountsOfNodes(suppliersCNDF,10))
        retailerCNDF = (self.ValidatethecountsOfNodes(retailerCNDF,2))
        warehousesCNDF = (self.ValidatethecountsOfNodes(warehousesCNDF,1))

        self.criticalNodesRespectToConnetedNodes(suppliersCNDF,retailerCNDF)
        
        suppliersCNDF.to_csv("suppliersCNDF.csv")
        retailerCNDF.to_csv("retailerCNDF.csv")
        warehousesCNDF.to_csv("warehousesCNDF.csv")
    

    # how many product does each warehouse has
    def criticalWarehouses(self,productName):
        command = '''match(n:Warehouses)-[]->(m:Products{name:'%s'}) return n.name''' %(productName)
        result = self.myGraph.execute_Command(command)
        warehouses = []
        for criticalNode in result:
            x = dict(criticalNode)
            warehouses.append(x['n.name'])
        return warehouses
   
   # should be called only once
    def addQuantityToProduct(self,nodes_df):
        import random as rd
        ProductsData = pd.read_csv("./DataSet/Products_data.csv")
        capacityDF = pd.DataFrame(columns=['capacity'])
        warehousesCapacity = []
        mainCapacity = []
        for product in range(len(ProductsData)):
            listOfEachProduct = ProductsData.loc[product]["warehouses"]
            listOfEachProduct = ast.literal_eval(listOfEachProduct)
            for warehouse in range(len(listOfEachProduct)):
                warehouseId = listOfEachProduct[warehouse]
                capacityOfWarehouse = self.gettingCapacityOfWarehouse(warehouseId,nodes_df)
                randomProductQuantity = rd.randint(5000,capacityOfWarehouse) #can warehouse capacity be less than 5000?
                warehousesCapacity.append(randomProductQuantity)
            mainCapacity.append(warehousesCapacity)
            warehousesCapacity = []
        capacityDF["capacity"] = mainCapacity
        ProductsData = pd.merge(
                ProductsData,
                capacityDF,
                how='left',
                left_index=True, 
                right_index=True 
                )
        print(ProductsData.head())
        ProductsData.to_csv("./DataSet/Products_data.csv",index=False)
    
    def addAttribureToProductsInNodesDF(self,nodes_df):
        nodes_df = nodes_df[nodes_df.Label == "products"]

        print()
    
    def gettingCapacityOfWarehouse(self,warehouseID,nodes_df):
        nodes_df = nodes_df[nodes_df.Label == "warehouses"]
        attributes = ((nodes_df[nodes_df.ID == warehouseID])["Attributes"])
        index = (attributes.index)[0]
        return attributes[index]['capacity (NA)']

    def criticalNodesRespectToConnetedNodes(self,suppliersCNDF,retailerCNDF):
        
        hisFigS = px.histogram(suppliersCNDF, x="followers")
        boxFigS = px.box(suppliersCNDF, x="followers")
        hisFigS.show()
        boxFigS.show()

        hisFigR = px.histogram(retailerCNDF, x="followers")
        boxFigR = px.box(retailerCNDF, x="followers")
        hisFigR.show()
        boxFigR.show()

        criticalSupplierIndices = list((self.find_outliers_IQR(suppliersCNDF["followers"])).index)
        criticalRetailerIndices = list((self.find_outliers_IQR(retailerCNDF["followers"])).index)
        suppliers = self.getCriticalsrespectTofollowers(criticalSupplierIndices,suppliersCNDF)
        retailers = self.getCriticalsrespectTofollowers(criticalRetailerIndices,retailerCNDF)

        return suppliers,retailers
    
    def getCriticalsrespectTofollowers(self,indices,df):
        criticalsNames = []
        for index in indices:
           criticalsNames.append(df.loc[index]['name'])
        return criticalsNames

    def find_outliers_IQR(self,df):

        q1=df.quantile(0.25)

        q3=df.quantile(0.75)

        IQR=q3-q1

        outliers = df[((df<(q1-1.5*IQR)) | (df>(q3+1.5*IQR)))]
    
        return outliers
    

    def validateConnectedNodes(self,nodeName):
        connectedNodes = self.getConnectedNodes(nodeName)
        facilitiesNodes = list(filter(lambda node: node.split(" ")[0] == "Facilities",connectedNodes))
        productsNodes = list(filter(lambda node: node.split(" ")[0] == "Products",connectedNodes))
        return (len(facilitiesNodes) + len(productsNodes))
    
        
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

    def ValidatethecountsOfNodes(self,DF,min):    
        return  DF[DF.followers>=min].reset_index(drop=True) 
     
    def getConnectedNodes(self,souceNodeName):
        nodeLabel = souceNodeName.split(" ")[0]
        command = '''match(n:%s {name:'%s'})-[]->(m) return n.name,m.name''' %(nodeLabel,souceNodeName)
        result = self.myGraph.execute_Command(command)
        resultNames = []
        for criticalNode in result:
            x = dict(criticalNode)
            resultNames.append(x['m.name'])
        return resultNames

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
   
    # Get CriticalNodes Respect to the product --- which the only node that supply specific product 
    # takes the source label "Supplier,Warehouses,Retailer", nodesTable,edgesDF, nodesDF
    def criticalNodesRespectToProduct(self,sourceLabel,nodesTable,edgesDF,nodes_df):
       # filter the edges table based on the source Label and the products
        filteredEdgesDF = (edgesDF[(edgesDF.From_Table == sourceLabel)&(edgesDF.To_Table == "Products")]).reset_index(drop=True) 
        # for future usages
        criticalNodesWithRespectToProduct = []
        repeatedProductsIndices = []
        # loop oveer the filtered edges df
        for node in range(len(filteredEdgesDF)):
            # check we checked on the current product or not
            if filteredEdgesDF.loc[node]['To'] in repeatedProductsIndices:
                # if so, skip what the code below
                continue
            # get the currect product
            To_Index = filteredEdgesDF.loc[node]['To'] 
            # filter the edges table based on the current produc index -- to check if there are more than node supply the current product
            temp = (filteredEdgesDF[(filteredEdgesDF.To == To_Index)])
           # if the current product doesn't occur again in the table
            if len(temp) == 1:
                # get the from node "Retailer,Supplier,Warehouse" from nodesTable based on the index mentioned on the egdesTable
                sourceID = nodesTable.loc[(filteredEdgesDF.loc[node]['From'])]['ID']
                # get the node name
                sourceNodeName = sourceLabel + str(sourceID)
                # append it on the list
                criticalNodesWithRespectToProduct.append(sourceNodeName)
            # if the current product occurs more than once in the edgesTable add it to the repeated products 
            else:
                repeatedProductsIndices.append(filteredEdgesDF.loc[node]['To'])
        return criticalNodesWithRespectToProduct
    
    def criticalNodesRespectToWeightAndPrices(self,sourceNodeName,targetNodeName,graphName):
        # criteria 1: using dijkstra's algorithm
        findAllPathsDic = {'targetNodeName': targetNodeName , 'cases': 2, 'graphName': graphName,'relationship':"",'k':1,'TargetType':""}
        validatPathsDic = {'nodesNames':self.nodeNames,'edgesNames':self.edgesNames,'nodesTable':self.nodeNames,"desiredType":""}
        theUniquePath = (self.graphAnalysis.mainMethod(sourceNodeName,True,findAllPathsDic,validatPathsDic))['nodeNames'][0]
        nodesWithTheirFollowers = self.getNodesWiththeCountsOfConnectedNodes(graphName,"UNDIRECTED") 
        followersList = []
        # criteria 2: check on connected Nodes
        edgesListCapital = list(map(lambda node: node.capitalize(), self.relationNames))
        result = list(filter(lambda node: ((node.split(" "))[0]) not in edgesListCapital,theUniquePath))
        for node in result:
            followers = ((nodesWithTheirFollowers[(nodesWithTheirFollowers.name == node)]).reset_index(drop=True))['followers'][0]
            followersList.append(followers)
        followersList.pop(0)
        followersList.pop(-1)
        return followersList
        
    '''
        for each prod in the dataset 
        access Products_data.csv using the prod_id
        from the dataset add capacity & warehouses list to the attributes
        '''

    def add_warehouses_capacity_to_nodes_df(self,nodes_df):
        products_data = pd.read_csv("./DataSet/Products_data.csv")
        # products_df = nodes_df[nodes_df["Label"]=="products"]
        for prod in range(len(products_data)):
            id = products_data.loc[prod]['prod_id']
            x = nodes_df[(nodes_df['Label']=="products") & (nodes_df['ID']==id)]
            index = x["Attributes"].index.to_list()[0]
            att_dict = x.Attributes.to_dict()
            att = att_dict[index]
            att_warehouses = products_data.loc[prod]['warehouses']
            att_capacity = products_data.loc[prod]['capacity']
            att["capacity"] = att_capacity
            att["warehouses"] = att_warehouses
            nodes_df.at[index,"Attributes"] = att
            # nodes_df.iloc[index]["Attributes"] = att
        nodes_df[nodes_df["Label"]=="products"].to_csv("checker.csv",index=False)

    def critical_warehouses_with_respect_to_count(self,nodes_df):
        '''
        '''
        self.add_warehouses_capacity_to_nodes_df(nodes_df)
        #{"prod_id":[critical_]}
        critical_warehouses = []
        prod_df = nodes_df[nodes_df['Label']=="products"]
        for prod in range(len(prod_df)):
            prod_id = "Product " + str(prod_df.iloc[prod]["ID"])
            warehouses_list = ast.literal_eval((prod_df.iloc[prod]["Attributes"])['warehouses'])
            if(len(warehouses_list)==1):
                critical_warehouses.append((prod_id, "Warehouse " + str(warehouses_list[0])))
        print(critical_warehouses)

    def critical_warehouses_with_respect_to_percentage(self, nodes_df):
        self.add_warehouses_capacity_to_nodes_df(nodes_df)
        critical_warehouses = []
        prod_df = nodes_df[nodes_df['Label']=="products"]
        for prod in range(len(prod_df)):
            prod_id = prod_df.iloc[prod]["ID"]
            warehouses_list = ast.literal_eval((prod_df.iloc[prod]["Attributes"])['warehouses'])
            capacity_list = ast.literal_eval((prod_df.iloc[prod]["Attributes"])['capacity'])
            if(len(capacity_list) > 1):
                capacity_percent_list = self.get_percentage(capacity_list)
                max_percent = max(capacity_percent_list)
                max_index = capacity_percent_list.index(max_percent)
                if(max_percent >= 50):
                    critical_warehouses.append(("Product " + str(prod_id),"Warehouse " + str(warehouses_list[max_index])))
            else:
                continue
        print(critical_warehouses)  

    def get_percentage(self,target_list):
        s = sum(target_list)
        res = []
        res = map(lambda x: (x / s)*100, target_list)
        return list(res)
        
