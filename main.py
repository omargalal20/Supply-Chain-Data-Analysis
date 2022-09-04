from pickle import TRUE
from Neo4jGraph import Neo4jGraph
from ReadingDataSet import ReadingDataSet
from keys import keys
from InitializingNodesAndEdges import InitializeNodesAndEdges
from nodes_edges_df import nodes_edges_dfs

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
# myGraph.draw_graph('supplyChainNew')
print("---------------Case 0-------------------")
#myGraph.findAllPaths(sourceNodeName="Supplier 90236",label="Supplier",cases=0,graphName="supplyChain91",relationShip="ssintship")
#print("---------------Case 1-------------------")
#print(myGraph.findAllPaths(sourceNodeName="Supplier 90236",label="Supplier",cases=1,graphName="supplyChain",relationShip="ssintship",k=4,targetNodeName="Supplier 90236"))
print("---------------Case2-------------------")
print(myGraph.findAllPaths(sourceNodeName="Supplier 90236",label="Supplier",cases=2,graphName="supplyChain",targetNodeName="Supplier 61565"))
#output = myGraph.trail(label="Warehouses",id=8750,nodeID="warehouses 8750")
#print(output)


#print(nodes_df.loc[(nodes_df['ID'] == 380 ) & (nodes_df['Label'] == 'supplier')])

#print(list(All_dfs.keys()))