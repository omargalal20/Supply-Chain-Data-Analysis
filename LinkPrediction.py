from neo4j import GraphDatabase
import numpy as np

class LinkPredictor:
    def __init__(self):

        self.__DBusername = "neo4j"
        self.__DBpassword = "123"
        self.__DBuri = "bolt://localhost:7687"
        self.__driver = GraphDatabase.driver(uri=self.__DBuri, auth=(self.__DBusername, self.__DBpassword))

        self.allExistingGraphs = self.getGraphs()

    def execute_Command(self, command):
        output = self.__driver.session().run(command)
        print("------------executed-----------------")
        return output

    def ExistingGraph(self, name):
        for n in self.allExistingGraphs:
            if (n == name):
                return True
        return False

    def saveGraph(self, name, nodeList, edgeList):
        saveStatment = '''
        CALL gds.graph.project(
        '%s',
        %s,
        %s,
        {
        relationshipProperties: 'weight'
        })
        YIELD
        graphName AS graph, nodeProjection, nodeCount AS nodes, relationshipCount AS rels
        ''' % (name, nodeList, edgeList)
        print("----------------GRAPH SAVINGGGG----------------")
        self.execute_Command(saveStatment)
        self.allExistingGraphs.append(name)
        print("----------------GRAPH SAVED----------------")

    def draw_graph(self, name):
        # check if the graph name doesn't exists in the database
        if (self.ExistingGraph(name) == False):
            # save the graph
            self.saveGraph(name, nodeList=['Customer', 'Products', 'Retailer', 'Supplier', 'Rcextship', 'Scextship',
                                           'Srintship', 'Ssintship', 'Facilities', 'Warehouses', 'Rcextorders',
                                           'Scextorders', 'Srintorders', 'Ssintorders', 'Externalservices',
                                           'Internalservices', 'Externaltransactions', 'Internaltransactions'],
                           edgeList=['Order', 'rcextship', 'scextship', 'srintship', 'ssintship', 'Related_To',
                                     'Manufactures', 'Orders_Prodcut', 'externaltransactions', 'internaltransactions'])
            print(self.allExistingGraphs)
        # if the graph exists, nothing happens
        else:
            print("Graph Already Exists")

    ## get all graphs names in the DB and save it in array (global variable) return array
    def getGraphs(self):
        # send the graph list command
        stat = "CALL gds.graph.list()"
        print(stat)
        graphs = self.execute_Command(stat)
        temp = []
        # loop on the graphs and convert it to dictionary and add it to the array
        for graph in graphs:
            x = dict(graph)
            temp.append(x['graphName'])
        # return the array of all graphs exists in the database
        print(temp)
        return temp
    
    # Using Queries For Link Prediction
    def getSuppliersOfCertainSupplier(self,supplierName):
        nodeStatement = f'''
        MATCH (s1:Supplier)<-[r1]-(ss:Ssintship)<-[r2]-(s2:Supplier) WHERE s1.name = {supplierName}
        MATCH (s2)-[r3]->(p:Products) RETURN s1 AS SelectedSupplier,ss,s2 AS Suppliers,p AS SuppliedProducts LIMIT 50
        '''
        output = self.execute_Command(nodeStatement)
        listOfSuppliers = []
        i = 0
        selectedSupplier = {}
        for record in output:
            # print(f"{n}".encode('utf-8'))
            if(i == 0):
                selectedSupplier = record['SelectedSupplier']
            listOfSuppliers.append({'Supplier': record['Suppliers'], 'Product': record['SuppliedProducts']})
            self.getSuppliersSupplyingSameProducts(record['SuppliedProducts']['product_type'])
        print(f'{selectedSupplier}'.encode('utf-8'))
        print(f'{listOfSuppliers}'.encode('utf-8'))
        print(len(listOfSuppliers))
        self.__driver.close()

    def getSuppliersSupplyingSameProducts(self,productType):
        print('2nd Method')
        nodeStatement = f'MATCH (n:Supplier)-[r]->(m:Products) WHERE m.product_type = "{productType}" RETURN n AS Supplier,m AS Product LIMIT 100'
        output = self.execute_Command(nodeStatement)
        for record in output:
            print(f"{record}".encode('utf-8'))
        self.__driver.close()
    
    # Using Algorithms For Link Prediction
    def adamicAdar(self):
        nodeStatement = 'MATCH (s1:Supplier) MATCH (s2:Supplier) RETURN gds.alpha.linkprediction.adamicAdar(s1, s2) AS score'
        output = self.execute_Command(nodeStatement)
        arr = np.array([record['score'] for record in output])
        print(np.unique(arr))
        # for n in output:
        #     if(n['score'] > 0):
        #         print(n)
        #         print(n['score'])
        self.__driver.close()

    def cosineSimilarity(self):
        nodeStatement = '''MATCH (c:Cuisine)
        WITH {item:id(c), weights: c.embedding} AS userData
        WITH collect(userData) AS data
        CALL gds.alpha.similarity.cosine.stream({
        data: data,
        skipValue: null
        })
        YIELD item1, item2, count1, count2, similarity
        RETURN gds.util.asNode(item1).name AS from, gds.util.asNode(item2).name AS to, similarity
        ORDER BY similarity DESC'''

trial = LinkPredictor()
# trial.draw_graph('TrialGraph')
trial.getSuppliersOfCertainSupplier('"Supplier 65468"')
# trial.getSuppliersSupplyingSameProducts('"Y"')
    