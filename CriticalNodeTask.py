import pandas as pd


class CriticalNodeTask:

    def __init__(self,myGraph,nodeNames):
        self.myGraph = myGraph
        self.nodeNames = nodeNames

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
        criticalNodesDF = self.ValidateCriticalNodes(criticalNodesDF,self.nodeNames)
        return criticalNodesDF

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
        print("begining")
        followersColumn = criticalNodesDF['followers']
        max = followersColumn.max()
        threshold = max - 10
        temp = criticalNodesDF
        for node in range(len(temp)):
            if criticalNodesDF.loc[node]['followers'] < threshold:
                criticalNodesDF = criticalNodesDF.drop(node)
        return criticalNodesDF.reset_index(drop=True) 

        

        
    