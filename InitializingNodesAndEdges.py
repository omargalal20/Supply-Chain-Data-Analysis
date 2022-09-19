class InitializeNodesAndEdges:

    def __init__(self, All_dfs, fk):
        self.All_dfs = All_dfs
        self.fk = fk
        self.nodes = []
        self.edges = []
        self.properties = []
        self.convert_using_naming()
        self.nodes_edges(fk)

    def convert_using_naming(self):
        nodesNames = ('supplier', 'customer',  'retailer', 'service providers', 'distributors', 'sales channels', 'consumers', 'producers', 'vendors', 'transportation', 'wholesaler', 'distribution center', 'warehouses')

        for name in list(self.All_dfs.keys()):
            lowerCasedName = name.lower()
            if(lowerCasedName in nodesNames):
                self.nodes.append(lowerCasedName)

    def nodes_edges(self, fk):
        for f in fk:
            if(f not in ['ssintorders','manufacturing']):
                if len(fk[f]) == 2:
                    self.edges.append(f)
                elif len(fk[f]) == 1:
                    self.properties.append(f)
                # elif len(fk[f]) == 0:
                #     nodes.add(f)