from warnings import catch_warnings
from CriticalNodeTask import CriticalNodeTask
from Neo4jGraph import Neo4jGraph
from ReadingDataSet import ReadingDataSet
from keys import keys
from InitializingNodesAndEdges import InitializeNodesAndEdges
from NodesEdgesManager import NodesEdgesManager
from GraphAnalysis import GraphAnalysis
import pandas as pd
from os.path import exists
import pickle

if not exists("Pickle Files/nodes_df.pkl"):

    dataSet = ReadingDataSet()
    # Dictionary containing all dataframes
    All_dfs = dataSet.All_dfs
    print(f"Df Keys: {All_dfs.keys()}")

    key = keys(All_dfs)
    # Dictionary indicating the column of each table that represents the primary key
    All_pks = key.primaryKeyGetter()
    print(f"Primary Keys: {All_pks}")
    print('-------------------------')
    # Dictionary indicating the column of each table that represents the foreign key and the referenced table name
    All_fks = key.foreignKeyGetter()
    print(f"Foreign Keys: {All_fks}")
    print('-------------------------')
    # Dictionary indicating which tables references the table
    All_ref_ins = key.ref_in
    print(f"Ref In Keys: {All_ref_ins}")
    print('-------------------------')

    initializingNodesAndEdges = InitializeNodesAndEdges(All_dfs, All_fks)
    nodes = initializingNodesAndEdges.nodes
    print(f"Nodes: {nodes}")
    with open("Pickle Files/nodes.pkl","wb") as f:
        pickle.dump(nodes,f)
    print('-------------------------')
    edges = initializingNodesAndEdges.edges
    with open("Pickle Files/edges.pkl","wb") as f:
        pickle.dump(edges,f)
    print(f"Edges: {edges}")
    print('-------------------------')
    properties = initializingNodesAndEdges.properties
    print(f"Properties: {properties}")
    with open("Pickle Files/properties.pkl","wb") as f:
        pickle.dump(properties,f)
    print('-------------------------')


    # True to output nodes table and edges table as a normal graph
    # False to output nodes table and edges table but edges are nodes
    initialize_nodes_edges_df = NodesEdgesManager(nodes, edges, properties, All_pks, All_fks, All_ref_ins, All_dfs, True)

    nodesTable = initialize_nodes_edges_df.nodesTable
    print("Nodes Table: ")
    nodesTable.to_csv('./CSV Files/nodesTable.csv')
    # print(nodesTable)
    print('-------------------------')


    edgesTable = initialize_nodes_edges_df.edgesTable
    print("Edges Table: ")
    edgesTable.to_csv('./CSV Files/edgesTable.csv')
    # print(edgesTable)
    print('-------------------------')

    initialize_nodes_edges_df = NodesEdgesManager(nodes, edges, properties, All_pks, All_fks, All_ref_ins, All_dfs, False)

    nodes_df = initialize_nodes_edges_df.nodes_df_edges_as_nodes
    print("Nodes DF: ")
    nodes_df.to_csv('./CSV Files/nodes_df.csv')
    print('-------------------------')


    edges_df = initialize_nodes_edges_df.edges_df_edges_as_nodes
    print("Edges DF: ")
    edges_df.to_csv('./CSV Files/edges_df.csv')
    # print(edges_df)
    print('-------------------------')

    nodesTable.to_pickle("Pickle Files/nodesTable.pkl")
    edgesTable.to_pickle("Pickle Files/edgesTable.pkl")
    nodes_df.to_pickle("Pickle Files/nodes_df.pkl")
    edges_df.to_pickle("Pickle Files/edges_df.pkl")

else:
    with open("Pickle Files/nodes.pkl","rb") as f:
        nodes = pickle.load(f)
    with open("Pickle Files/edges.pkl","rb") as f:
        edges = pickle.load(f)
    with open("Pickle Files/properties.pkl","rb") as f:
        properties = pickle.load(f)
    nodes_df = pd.read_pickle("Pickle Files/nodes_df.pkl")
    edges_df = pd.read_pickle("Pickle Files/edges_df.pkl")
    nodesTable = pd.read_pickle("Pickle Files/nodesTable.pkl")
    edgesTable = pd.read_pickle("Pickle Files/edgesTable.pkl")
   
try:
    myGraph = Neo4jGraph(nodes_df,edges_df)
    # myGraph.populate_database()
    #myGraph.draw_graph("supplyChain4")
    graphAnalysis = GraphAnalysis(myGraph,nodesTable,edgesTable)
    # z.to_csv("./CSV Files/nodesFilter.csv")
    criticalNodeTask = CriticalNodeTask(myGraph,graphAnalysis,nodes,edges,nodes)
    x = criticalNodeTask.getNodesWiththeCountsOfConnectedNodes('supplyChain','undirected')
    print(criticalNodeTask.criticalNodesRespectToConnetedNodes(x))
    print(criticalNodeTask.getCriticalNodesRespectToLocation(nodesTable))
    findAllPathsSet = {'targetNodeName': "Supplier 48580" , 'cases': 1, 'graphName': "supplyChain",'relationship':"",'k':4,'TargetType':""}
    validaPathsSet = {'nodesNames':nodes,'edgesNames':edges,'nodesTable':nodesTable,"desiredType":"Chemicals"}
    # x = graphAnalysis.mainMethod('Supplier 65468',True,findAllPathsSet,validaPathsSet)
    # x.to_csv('./CSV Files/result.csv')
except  Exception as e: print(e)
finally:
    print("Terminating")
    myGraph.close()

##Supplier 65468
# x = graphAnalysis.mainMethod("Supplier 65468",True,findAllPathsSet,validaPathsSet)
# x.to_csv("lastcheck")

#myGraph.draw_graph("supplyChain")

#graphAnalysis.findAllPathsViseVerse('Retailer','Retailer 83982','Supplier','Chemicals',nodesTable,'supplyChain',nodes,edges)

##graphAnalysis.findAllPathsViseVerse(SourceLabel='Customer',SourceNodeName='Customer 48071',TargetLabel='Supplier',nodesTable=nodesTable,graphName='SupplyChain',nodeNames=nodes,edgesNames=edges,TargetType='Chemicals')

# x = ['Supplier 20226', 'Ssintship 10465', 'Supplier 30864', 'Ssintship 10482', 'Supplier 18952', 'Ssintship 10173', 'Supplier 13951', 'Ssintship 10350', 'Supplier 21232', 'Internaltransactions 9683', 'Supplier 78265', 'Ssintship 10290']
# y = ['Supplier 20226', 'Ssintship 10465', 'Supplier 30864', 'Ssintship 10482', 'Supplier 18952', 'Ssintship 10173', 'Supplier 13951', 'Ssintship 10350', 'Supplier 21232', 'srintorders 9683', 'Supplier 78265', 'Ssintship 10290']
# z = ['Supplier 20226', 'Ssintship 10113', 'Supplier 54148']
# m = ['Supplier 20226', 'Ssintship 10206', 'Supplier 68814']
# o = ['Supplier 20226', 'Ssintship 10206', 'srintorders 9683','Supplier 68814']
# t = ['Supplier 20226', 'Ssintship 10206', 'Supplier 68814','Ssintship 10482','Supplier 68814' ]
# temp = pd.DataFrame()
# final = pd.DataFrame()
# tmp = pd.DataFrame({'index': [1,2,3,4,5,6],
#                     'sourceNodeName': ["Supplier 20226","Supplier 20226","Supplier 20226","Supplier 20226","Supplier 20226","Supplier 20226"],
#                     'targetNodeName': ["Ssintship 10290","Ssintship 10291","Supplier 54148",'Supplier 68814','Supplier 68814','Supplier 68814'],
#                     'totalCost': [20,21,44,55,66,99]})

# costs_df = pd.DataFrame({'costs': [[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4]]})
# result = pd.merge(
#                  tmp,
#                  costs_df,
#                  how='left',
#                  left_index=True, 
#                  right_index=True 
#                  )
          
# nodeNames_df = pd.DataFrame({'nodeNames': [x,y,z,m,o,t]})
# temp = pd.merge(
#                 result,
#                 nodeNames_df,
#                 how='left',
#                 left_index=True, 
#                 right_index=True 
#             )
# final = pd.concat([final,temp], ignore_index=True)


#graphAnalysis.lastCheckOnPath(dataFrameOfPaths=final,nodeTables=nodesTable)

#myGraph.draw_graph('supplyChain1')
#print(myGraph.allExistingGraphs)

#print("---------------Case 0-------------------")
# output = graphAnalysis.findAllPaths(sourceNodeName="Supplier 65468",cases=0,graphName="supplyChain")
# out = graphAnalysis.validatePath(paths=output,sourceNodeName="Supplier 65468",nodeNames=nodes,edgesNames=edges,nodesTable=nodesTable,theDesiredType="Paper, Forest Products & Packaging")
# out.to_csv("result.csv")
#print(type(nodesTable))
#nodesTable[(nodesTable.Label == "Supplier")&(nodesTable.ID == 34967)]
#print(output)
#xp = graphAnalysis.targetNodeValidation(paths=final,sourceNodeName="Supplier 20226",nodeNames=nodes)
#x = graphAnalysis.targetNodeValidation(paths=xp,sourceNodeName="Supplier 20226",nodeNames=nodes)
#graphAnalysis.validatePath(output,"Supplier 11790",nodes,edges)
#print("---------------Case 1-------------------")
#tput = graphAnalysis.findAllPaths(sourceNodeName="Supplier 65468",cases=1,graphName="supplyChain",k=4,targetNodeName="Supplier 48580",targetLabel="Supplier")
# out = graphAnalysis.validatePath(paths=output,sourceNodeName="Supplier 65468",nodeNames=nodes,edgesNames=edges)
# out.to_csv("out.csv")
# result = graphAnalysis.lastCheckOnPath(out,nodesTable,'Paper, Forest Products & Packaging')
# result.to_csv("trail5.csv")
#graphAnalysis.lastCheckOnPath(dataFrameOfPaths=out,nodeTables=nodesTable,theDesiredType="Financial Services")
#print(graphAnalysis.targetNodeValidation(paths=output,sourceNodeName="Retailer 83982",nodeNames=nodes,targetNodeName="Customer 31548"))
#print("---------------Case2-------------------")
#print(graphAnalysis.findAllPaths(sourceNodeName="Supplier 34967",cases=2,graphName="supplyChain",targetNodeName="Supplier 14125",targetLabel="Supplier"))
#output = myGraph.trail(label="Warehouses",id=8750,nodeID="warehouses 8750")
#print(output)
#print(myGraph.getGraphs())
#print(nodes_df.loc[(nodes_df['ID'] == 380 ) & (nodes_df['Label'] == 'supplier')])
#print(list(All_dfs.keys()))
