from gephistreamer import graph
from gephistreamer import streamer
import random as rn

class GephiGraph:

    def __init__(self, All_dfs):
        self.All_dfs = All_dfs
        self.nodes = set()
        self.edges = set()
        self.properties = set()
    
    def draw_graph_Gephi(self, nodes_table,edges_table, workSpaceName):
        stream = streamer.Streamer(streamer.GephiWS(hostname="localhost",port=8080,workspace="trial1"))

        szfak = 100  # this scales up everything - somehow it is needed
        cdfak = 3000

        nodedict = {}

        colors = {'supplier': "#8080ff",'retailer': "#ff8000",'customer': "#ff80ff"}
        xPos = {'supplier': -3000,'retailer': 0,'customer': 3000}

        for nodeIndex in range(len(nodes_df)):
            string = str(nodesTable.iloc[nodeIndex]['Attributes']).split(',')
            conc = ""
            for i in range(len(string)):
                conc += string[i] + '\n'
                
            node = graph.Node(nodeIndex,custom_property=conc,size=szfak,x=cdfak*rn.random(),y=cdfak*rn.random(),color=colors[nodesTable.iloc[nodeIndex]['Label']],type=nodesTable.iloc[nodeIndex]['Label'])
            stream.add_node(node)

        for edgeIndex in range(len(edges_df)):
            string = str(edgesTable.iloc[edgeIndex]['order/service']).split(',')
            conc = ""
            for i in range(len(string)):
                conc += string[i] + '\n'
            edge_ab = graph.Edge(edgesTable.iloc[edgeIndex]['From'],edgesTable.iloc[edgeIndex]['To'],custom_property=conc)
            stream.add_edge(edge_ab)