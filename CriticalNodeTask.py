import pandas as pd


class CriticalNodeTask:

    def __init__(self,myGraph,graphAnalysis,nodeNames,edgesNames,nodesTable):
        self.myGraph = myGraph
        self.graphAnalysis = graphAnalysis
        self.nodeNames = nodeNames
        self.edgesNames = edgesNames
        self.nodesTable = nodesTable
        self.criticalNodesDF = pd.DataFrame()


    def getCriticalNodes(self,graphName,orientation=""):
        executedStatment = ""
        if orientation == "":
            executedStatment = '''CALL gds.degree.stream('%s')
                                    YIELD nodeId, score
                                    RETURN gds.util.asNode(nodeId).name AS name, score AS followers
                                    ORDER BY followers DESC, name DESC
                                    '''%(graphName)
        else:
           executedStatment = '''CALL gds.degree.stream('%s',{orientation:'%s'})
                                YIELD nodeId, score
                                RETURN gds.util.asNode(nodeId).name AS name, score AS followers
                                ORDER BY followers DESC, name DESC
                                '''%(graphName,orientation)
        criticalNodes = self.myGraph.execute_Command(executedStatment)
        criticalNodesDF = self.returnCriticalNodesDF(criticalNodes)
        self.criticalNodesDF = self.ValidateCriticalNodes(criticalNodesDF,self.nodeNames)
        print(self.criticalNodesDF)
        return self.criticalNodesDF

    def returnCriticalNodesDF(self,criticalNodes):
        criticalNodesDF = pd.DataFrame()
        i = 0
        for criticalNode in criticalNodes:
            x = dict(criticalNode)
            nodeName = x['name']
            nodeFollowers = x['followers']
            temp = pd.DataFrame({'name':[nodeName],
                                'followers':[nodeFollowers]})
            criticalNodesDF = pd.concat([criticalNodesDF,temp],ignore_index=True)
            i+=1
        return criticalNodesDF

    def ValidateCriticalNodes(self,criticalNodesDF,nodeNames):
        temp = criticalNodesDF
        for node in range(len(temp)):
            nodeLabel = (((criticalNodesDF.loc[node]['name']).split(" "))[0]).lower()
            nodeFollowers = criticalNodesDF.loc[node]['followers']
            if (nodeLabel not in nodeNames) | (nodeFollowers == 0):
                criticalNodesDF = criticalNodesDF.drop(node)
        return criticalNodesDF.reset_index(drop=True) 

    def criticalNodesRespectToGraph(self,criticalNodesDF):
        followersColumn = criticalNodesDF['followers']
        max = followersColumn.max()
        threshold = max - 10
        temp = criticalNodesDF
        for node in range(len(temp)):
            if criticalNodesDF.loc[node]['followers'] < threshold:
                criticalNodesDF = criticalNodesDF.drop(node)
        criticalNodeDF = criticalNodesDF.reset_index(drop=True) 
        return criticalNodeDF

    # use criticalNodeDF global variable --- ## return list of critical nodes that I can reach
    def ifIReachCriticalNode(self,sourceNodeName,graphName):
        if self.criticalNodesDF.empty:
            print("there is no critical Nodes")
            return
        # organizing attributes
        findAllPathsDic = {'targetNodeName': "" , 'cases': 0, 'graphName': graphName,'relationship':"",'k':1,'TargetType':""}
        validatPathsDic = {'nodesNames':self.nodeNames,'edgesNames':self.edgesNames,'nodesTable':self.nodeNames,"desiredType":""}
        # get all paths 
        allPaths = self.graphAnalysis.mainMethod(sourceNodeName,True,findAllPathsDic,validatPathsDic)
        allPaths.to_csv('./CSV Files/allPaths.csv')
        reachableCriticalNodes = []
        # loop ver all paths
        for path in range(len(allPaths)):
            # get node names of the current path
            nodeNamesOfCpath = (allPaths).loc[path]['nodeNames'] 
            # loop over critical nodes 
            for cNode in range(len(self.criticalNodesDF)):
                # get name of critical node
                criticalNode = (self.criticalNodesDF).loc[cNode]['name'] 
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

        
    def getCriticalNodesRespectToLocation(self,nodesTable):
        nodes = (nodesTable[nodesTable.Label == 'supplier']).reset_index(drop=True) 
        locationsDF = pd.DataFrame()
        locationsList = []
        for node in range(len(nodes)):
            location = (nodes.loc[node]['Attributes'])[2]
            locationsList.append(location)
        locations = pd.DataFrame({"a":locationsList})
        locations = locations.value_counts()
        locations = locations[locations == 1]
        