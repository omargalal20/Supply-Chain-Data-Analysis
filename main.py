from asyncore import read
from pickle import TRUE
from wsgiref import validate
#from turtle import pd
from Neo4jGraph import Neo4jGraph
from ReadingDataSet import ReadingDataSet
from keys import keys
from InitializingNodesAndEdges import InitializeNodesAndEdges
from nodes_edges_df import nodes_edges_dfs
from GraphAnalysis import GraphAnalysis
import pandas as pd

dataSet = ReadingDataSet()
# Dictionary containing all dataframes
All_dfs = dataSet.All_dfs
# print(f"Df Keys: {All_dfs}")

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
print('-------------------------')
edges = initializingNodesAndEdges.edges
print(f"Edges: {edges}")
print('-------------------------')
properties = initializingNodesAndEdges.properties
print(f"Properties: {properties}")
print('-------------------------')


# True to output nodes table and edges table as a normal graph
# False to output nodes table and edges table but edges are nodes
initialize_nodes_edges_df = nodes_edges_dfs(nodes, edges, properties, All_pks, All_fks, All_ref_ins, All_dfs, True)

nodesTable = initialize_nodes_edges_df.nodesTable
print("Nodes Table: ")
print(nodesTable)
print('-------------------------')


edgesTable = initialize_nodes_edges_df.edgesTable
print("Edges Table: ")
print(edgesTable)
print('-------------------------')

initialize_nodes_edges_df = nodes_edges_dfs(nodes, edges, properties, All_pks, All_fks, All_ref_ins, All_dfs, False)

nodes_df = initialize_nodes_edges_df.nodes_df_edges_as_nodes
print("Nodes DF: ")
print(nodes_df)
print('-------------------------')


edges_df = initialize_nodes_edges_df.edges_df_edges_as_nodes
print("Edges DF: ")
print(edges_df)
print('-------------------------')

myGraph = Neo4jGraph(nodes_df,edges_df)
graphAnalysis = GraphAnalysis(nodes_df,edges_df,nodesTable,edgesTable)


#graphAnalysis.findAllPathsViseVerse('Retailer','Retailer 83982','Supplier','Chemicals',nodesTable,'supplyChain',nodes,edges)

graphAnalysis.findAllPathsViseVerse(SourceLabel='Customer',SourceNodeName='Customer 48071',TargetLabel='Supplier',nodesTable=nodesTable,graphName='supplyChain',nodeNames=nodes,edgesNames=edges,TargetType='Chemicals')

x = ['Supplier 20226', 'Ssintship 10465', 'Supplier 30864', 'Ssintship 10482', 'Supplier 18952', 'Ssintship 10173', 'Supplier 13951', 'Ssintship 10350', 'Supplier 21232', 'Internaltransactions 9683', 'Supplier 78265', 'Ssintship 10290']
y = ['Supplier 20226', 'Ssintship 10465', 'Supplier 30864', 'Ssintship 10482', 'Supplier 18952', 'Ssintship 10173', 'Supplier 13951', 'Ssintship 10350', 'Supplier 21232', 'srintorders 9683', 'Supplier 78265', 'Ssintship 10290']
z = ['Supplier 20226', 'Ssintship 10113', 'Supplier 54148']
m = ['Supplier 20226', 'Ssintship 10206', 'Supplier 68814']
o = ['Supplier 20226', 'Ssintship 10206', 'srintorders 9683','Supplier 68814']
t = ['Supplier 20226', 'Ssintship 10206', 'Supplier 68814','Ssintship 10482','Supplier 68814' ]
temp = pd.DataFrame()
final = pd.DataFrame()
tmp = pd.DataFrame({'index': [1,2,3,4,5,6],
                    'sourceNodeName': ["Supplier 20226","Supplier 20226","Supplier 20226","Supplier 20226","Supplier 20226","Supplier 20226"],
                    'targetNodeName': ["Ssintship 10290","Ssintship 10291","Supplier 54148",'Supplier 68814','Supplier 68814','Supplier 68814'],
                    'totalCost': [20,21,44,55,66,99]})

costs_df = pd.DataFrame({'costs': [[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4]]})
result = pd.merge(
                 tmp,
                 costs_df,
                 how='left',
                 left_index=True, 
                 right_index=True 
                 )
          
nodeNames_df = pd.DataFrame({'nodeNames': [x,y,z,m,o,t]})
temp = pd.merge(
                result,
                nodeNames_df,
                how='left',
                left_index=True, 
                right_index=True 
            )
final = pd.concat([final,temp], ignore_index=True)


#graphAnalysis.lastCheckOnPath(dataFrameOfPaths=final,nodeTables=nodesTable)

#myGraph.draw_graph('supplyChain1')
#print(myGraph.allExistingGraphs)

#print("---------------Case 0-------------------")
#output = graphAnalysis.findAllPaths(sourceNodeName="Supplier 11790",sourceLabel="Supplier",cases=0,graphName="supplyChain")


# print(type(nodesTable))
#nodesTable[(nodesTable.Label == "Supplier")&(nodesTable.ID == 34967)]
#print(output)
#xp = graphAnalysis.targetNodeValidation(paths=final,sourceNodeName="Supplier 20226",nodeNames=nodes)
#x = graphAnalysis.targetNodeValidation(paths=xp,sourceNodeName="Supplier 20226",nodeNames=nodes)
#graphAnalysis.validatePath(output,"Supplier 11790",nodes,edges)
#print("---------------Case 1-------------------")
output = graphAnalysis.findAllPaths(sourceNodeName="Supplier 34967",sourceLabel="Supplier",cases=1,graphName="supplyChain",k=4,targetNodeName="Customer 31548",targetLabel="Customer")
out = graphAnalysis.validatePath(paths=output,sourceNodeName="Supplier 34967",nodeNames=nodes,edgesNames=edges)
graphAnalysis.lastCheckOnPath(dataFrameOfPaths=out,nodeTables=nodesTable,theDesiredType="Financial Services")
#print(graphAnalysis.targetNodeValidation(paths=output,sourceNodeName="Retailer 83982",nodeNames=nodes,targetNodeName="Customer 31548"))
#print("---------------Case2-------------------")
#print(graphAnalysis.findAllPaths(sourceNodeName="Supplier 34967",sourceLabel="Supplier",cases=2,graphName="supplyChain",targetNodeName="Supplier 14125",targetLabel="Supplier"))
#output = myGraph.trail(label="Warehouses",id=8750,nodeID="warehouses 8750")
#print(output)
#print(myGraph.getGraphs())
#print(nodes_df.loc[(nodes_df['ID'] == 380 ) & (nodes_df['Label'] == 'supplier')])
#print(list(All_dfs.keys()))