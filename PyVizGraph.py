from pyvis.network import Network


class PyVizGraph:

    def __init__(self, nodes_df, edges_df):
        self.nodes_df = nodes_df
        self.edges_df = edges_df

    def draw_graph(self):
        self.nodes_name = self.__get_nodes_names()
        self.indices = self.__get_nodes_indices()
        self.nodes_attributes = self.__get_nodes_attributes()
        self.nodes_color = self.__get_nodes_colors()

        self.__graph = Network(width='100%')
        #     g.barnes_hut()

        self.__add_nodes()
        self.__add_edges()
        # print("Number of Nodes: ", len(graph.get_nodes()))
        # print("Number of Edges: ", len(graph.get_edges()))

        self.graph.show('pyVizVizual.html')

    def __add_nodes(self):
        self.graph.add_nodes(self.indices,
                             title=self.nodes_attributes,
                             label=self.nodes_name,
                             color=self.nodes_color,
                             )

    def __add_edges(self):
        for edge in range(len(self.edges_df)):
            src = int(self.edges_df.iloc[edge]["From"])
            dst = int(self.edges_df.iloc[edge]["To"])
            self.graph.add_edge(src, dst, weight=5)

    def __get_nodes_names(self):
        nodes_names = [(nodeIdLable[1] + '_' + str(nodeIdLable[0])).capitalize() for nodeIdLable in
                       zip(self.nodes_df.ID, self.nodes_df.Label)]
        return nodes_names

    def __get_nodes_indices(self):
        nodes_indices = list(map(lambda nodeIdLable: int(nodeIdLable), list(self.nodes_df.index)))
        return nodes_indices

    def __get_nodes_attributes(self):
        nodes_attributes = list(
            map(lambda nodeIdLable: str(nodeIdLable).replace(',', "\n"), list(self.nodes_df.Attributes)))
        return nodes_attributes

    def __get_nodes_tables_names(self):
        table_names = list(self.nodes_df.Label.unique())
        return table_names

    import random
    def __get_nodes_colors(self):
        import random

        nodes_tables = self.__get_nodes_tables_names()
        colors = list(
            map(lambda i: "#" + "%06x" % random.randint(0, 0xFFFFFF), range(len(nodes_tables))))

        nodes_colors = list(map(lambda x: colors[nodes_tables.index(x)], list(self.nodes_df.Label)))

        return nodes_colors
