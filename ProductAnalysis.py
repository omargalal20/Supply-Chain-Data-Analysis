import pandas as pd


class ProductAnalysis:
    def __init__(self, myGraph):
        self.myGraph = myGraph
        self.supplier_product_status = pd.DataFrame(
            columns=["supplier_id", "supplier_name", "product_id", "product_name", "Total Manufacturing Cost",
                     "Total Sales", "Profit/Loss", "Target", "manufacturing_cost", "product_price"])

    def get_competitors(self, sup_id, prod_id):
        command = '''
            MATCH (s:Supplier)
            WHERE s.ID = %d
            MATCH (p: Products)
            WHERE p.ID = %d
            MATCH (competitor: Supplier ) --> (p2:Products)
            WHERE s.ID <> competitor.ID and s.type = competitor.type and p.product_type = p2.product_type
            RETURN competitor.ID, p2.ID
        ''' % sup_id, prod_id

    def get_all_nodes_under(self, supplier_id):
        command = '''
            MATCH (s:Supplier {ID:%d})
            CALL apoc.path.subgraphAll(s,{ relationshipFilter: "<ssintship|<rcextship|<srintship|<scextship"})
            YIELD nodes
            unwind nodes as node
            with node,  labels(node)[0] as label
            where node:Customer or node:Supplier or node:Retailer  
            return label, node.ID ORDER BY label ASC
        ''' % supplier_id
        return self.myGraph.execute_Command(command)

    def get_all_nodes_above(self, supplier_id):
        command = '''
                    MATCH (s:Supplier {ID:%d})
                    CALL apoc.path.subgraphAll(s,{ relationshipFilter: "ssintship>|rcextship>|srintship>|scextship>"})
                    YIELD nodes
                    unwind nodes as node
                    with node,  labels(node)[0] as label
                    where node:Customer or node:Supplier or node:Retailer  
                    return label, node.ID ORDER BY label ASC
                ''' % supplier_id
        return self.myGraph.execute_Command(command)

    # Get All suppliers who manufacture product(s)
    def get_all_manufacturers(self):
        command = '''MATCH (s:Supplier)-[:Manufatures]->(:Products) 
                    Return DISTINCT s.ID AS id, s.name AS Name
                    Order BY id '''
        return self.myGraph.execute_Command(command)

    def analyze_supplier_products(self, supplier_id):
        all_products_details = self.get_sales_details(supplier_id)

        for product_details_dic in all_products_details:
            for key in product_details_dic:
                product_details = product_details_dic[key]
                product_id = product_details["product_id"]
                product_name = product_details["product_name"]
                product_price = product_details["product_price"]
                supplier_id = product_details["supplier_id"]
                supplier_name = product_details["supplier_name"]
                total_sales = product_details["total_sales"]
                total_quantity = product_details["total_quantity"]
                manufacturing_cost = product_details["manufacturing_cost"]
                product_yield = product_details["yield"]
                profit_target = product_details["target"]
                # details=product_details["details"]

                # yield * number_of_manufactured_products = quantity
                number_of_manufactured_products = int(total_quantity / product_yield)
                total_manufacturing_cost = number_of_manufactured_products * manufacturing_cost
                profit_or_loss = total_sales - total_manufacturing_cost
                actual_target = int(total_manufacturing_cost * profit_target * 100) / 100

                new_row = [supplier_id, supplier_name, product_id, product_name, total_manufacturing_cost, total_sales,
                           profit_or_loss, actual_target, manufacturing_cost, product_price]

                self.supplier_product_status.loc[len(self.supplier_product_status)] = new_row

        self.supplier_product_status.to_csv("./CSV Files/product_status.csv")

    def analayze_all_suppliers_products(self):
        suppliers = self.get_all_manufacturers()

        for supplier in suppliers:
            supplier_id = supplier["id"]
            supplier_name = supplier["Name"]
            self.analyze_supplier_products(supplier_id)
        # prodcut_IDs = self.myGraph.get_ids_of_connected_nodes(supplier_id, 'Supplier', 'Manufatures', 'Products')[0][
        #     "Result"]
        # print(prodcut_IDs)
        # for product_ID in prodcut_IDs:
        #     print(product_ID)

    def get_sales_details(self, supplier_id):
        command = \
            '''match (sup:Supplier )
                where sup.ID = %d
                match (sup)-[m:Manufatures]->(p:Products)
                match (sup)-->(:Ssintship)-->(o:Ssintorders)-->(p)
                match (o)<--(:Ssintship)-->(target:Supplier)
                with sup.ID AS sup_id, sup.supplier_name AS supplier_name,p.product_name AS product_name,
                p.ID AS product_ID, p.price AS product_price, 
                collect({order_id:o {.ID},recipient:target{.supplier_name,.ID}}) AS details,
                sum(o.quantity) As total_quantity, sum(toFloat(o.cost)) as total_cost,m.ManufacturingCost As cost,
                m.Target As target,m.Yield as yield

                return  {
                    supplier_id: sup_id,
                    supplier_name: supplier_name,
                    product_id:product_ID,
                    product_name:product_name,
                    product_price:product_price,
                    details:details,
                    total_quantity:total_quantity,total_sales: total_cost,
                    target: target, yield: yield,manufacturing_cost:cost
                }
    
        ''' % supplier_id
        return self.myGraph.execute_Command(command)
