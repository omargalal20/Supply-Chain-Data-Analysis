import os

import pandas
import pandas as pd
import random
import numpy as np
from functools import reduce
import spacy
from CheckSynonymForColumns import ConvertColumnsUsingSynonym


class ReadingDataSet:

    def __init__(self):
        self.All_dfs = {}
        self.createDataframes()
        self.removeNans()
        self.handleNonAtomicColumns()
        self.createMissingShipments()
        self.createMissingOrders()
        self.splittingShipmentsTables()
        self.splittingOrdersTables()
        self.removeRedundantTables()
        # self.__add_products_to_ssintorders()
        # self.__add_products_to_srintorders()
        self.__add_products_to_internal_orders()
        self.__add_type_and_products_sold_to_retailer()

        self.__add_products_to_rcextorders()
        self.__add_products_to_scextorders()

        self.__add_manufacturing_price()

    def createDataframes(self):
        path_of_the_directory = './DataSet/'
        ext = ('.csv')
        nlp = spacy.load('en_core_web_md')
        for file in os.listdir(path_of_the_directory):
            if file.endswith(ext):
                # print(file)
                temp = (file.replace("_", " ").replace(".", " ").split(" ")[0].lower())
                self.All_dfs[temp] = pd.read_csv(path_of_the_directory + file)
                # Adjust Naming Convention For Columns
                convertNaming = ConvertColumnsUsingSynonym(self.All_dfs, temp, nlp)
                self.All_dfs[temp].columns = convertNaming.convertedColumn
            else:
                continue
        print(self.All_dfs["internalorders"].columns)

    def and_agg(self, series):
        return reduce(lambda x, y: x and y, series)

    def handleNonAtomicColumns(self):
        for table_nm in self.All_dfs:
            table = self.All_dfs[table_nm]
            for column_name in table.columns:
                column = table[column_name]
                if column.dtype == 'O' and isinstance(column[0], str):
                    if (isinstance(column[0], str) and column.apply(
                            lambda x: (str(x).startswith('[') and str(x).endswith(']'))
                                      or
                                      (str(x).startswith('(') and str(x).endswith(')'))).agg(self.and_agg)):
                        print(table_nm, column_name)

                        s = column.apply(lambda x: x.strip("[](,)").split(','))
                        if (s[0][0].isdigit()):

                            new_s = pd.Series(dtype=object)
                            for index, item in s.iteritems():
                                new_item = []
                                for element in item:
                                    new_item.append(np.int64(element))
                                new_s.at[index] = new_item
                            s = new_s

                        else:
                            new_s = pd.Series(dtype=object)
                            for index, item in s.iteritems():
                                new_item = []
                                for element in item:
                                    new_item.append(element.strip().strip('"').strip("'").strip())
                                new_s.at[index] = new_item
                            s = new_s

                        self.All_dfs[table_nm][column_name] = s

    def createMissingShipments(self):
        c = 9995

        for r in range(len(self.All_dfs["manufacturing"])):
            diff_supp = self.All_dfs["manufacturing"].loc[r, "Different_suppliers"]
            factory = self.All_dfs["manufacturing"].loc[r, "Factory_id"]
            for s in range(len(diff_supp)):

                supp = diff_supp[s]
                if supp == factory:
                    print(factory)
                if len(self.All_dfs["internalshipments"].query(
                        f"listSuppIds == {supp} and factoryIds == {factory}")) == 0:
                    new_row = [{"IntShip_id": c, "listSuppIds": supp, "factoryIds": factory, "from_to_where": "SS"}]
                    df = pd.DataFrame(new_row)
                    c = c + 1
                    self.All_dfs["internalshipments"] = pd.concat([self.All_dfs["internalshipments"], df],
                                                                  ignore_index=True)

    def createMissingOrders(self):
        orders_int_ship_id = self.All_dfs["internalorders"].IntShip_id
        internal_ship_id = self.All_dfs["internalshipments"].IntShip_id
        ship_id_in_orders_ship_id = internal_ship_id.isin(orders_int_ship_id)
        ship_id_not_in_orders_ship_id = list(
            self.All_dfs["internalshipments"].IntShip_id[[not elem for elem in ship_id_in_orders_ship_id]].unique())

        c = 9931
        for id in ship_id_not_in_orders_ship_id:
            new_row = [{"IntOrders_id": c, "IntShip_id": id, "quantity": (random.randint(1, 1000)),
                        "placed_when": "1993-05-08",
                        "actual_date": "1993-05-30 00:00:00", "expected_date": "1993-06-01", "cost": 38.90,
                        "status": "Closed"}]
            df = pd.DataFrame(new_row)
            c = c + 1
            self.All_dfs["internalorders"] = pd.concat([self.All_dfs["internalorders"], df], ignore_index=True)

    def addTypeOfTransportationToShipments(self):
        IntShip = self.All_dfs["internalshipments"].shape[0]

        ExtShip = self.All_dfs["externalshipments"].shape[0]

        types = ['Sea', 'Land', 'Air']

        a = np.array([random.choice(types) for x in range(IntShip)])
        b = np.array([random.choice(types) for x in range(ExtShip)])

        seriesForIntShip = pd.Series(a)
        seriesForExtShip = pd.Series(b)

        self.All_dfs["internalshipments"]['TransportationType'] = seriesForIntShip
        self.All_dfs["internalshipments"].to_csv('DataSet/InternalShipments_data.csv', index=False)

        self.All_dfs["externalshipments"]['TransportationType'] = seriesForExtShip
        self.All_dfs["externalshipments"].to_csv('DataSet/ExternalShipments_data.csv', index=False)

    # def addOrderWeight(self):
    #     IntOrders = self.All_dfs["internalorders"].shape[0]

    #     ExtOrders = self.All_dfs["externalorders"].shape[0]

    #     a = np.array([random.choice(types) for x in range(IntOrders)])
    #     b = np.array([random.choice(types) for x in range(ExtOrders)])

    #     seriesForIntShip = pd.Series(a)
    #     seriesForExtShip = pd.Series(b) 

    #     self.All_dfs["internalshipments"]['TransportationType'] = seriesForIntShip

    #     self.All_dfs["externalshipments"]['TransportationType'] = seriesForExtShip

    def splittingShipmentsTables(self):
        SRIntShip = self.All_dfs["internalshipments"].query('from_to_where == "SR"')
        SRIntShip = SRIntShip.drop(['from_to_where'], axis=1).reset_index(drop=True)
        # SRIntShip
        RCExtShip = self.All_dfs["externalshipments"].query('from_to_where == "RC"')
        RCExtShip = RCExtShip.drop(['from_to_where'], axis=1).reset_index(drop=True)
        # RCExtShip
        SSIntShip = self.All_dfs["internalshipments"].query('from_to_where == "SS"')
        SSIntShip = SSIntShip.drop(['from_to_where'], axis=1).reset_index(drop=True)
        # SSIntShip
        SCExtShip = self.All_dfs["externalshipments"].query('from_to_where == "SC"')
        SCExtShip = SCExtShip.drop(['from_to_where'], axis=1).reset_index(drop=True)
        # SCExtShip
        self.All_dfs["RCExtShip".lower()] = RCExtShip
        self.All_dfs["SCExtShip".lower()] = SCExtShip
        self.All_dfs["SRIntShip".lower()] = SRIntShip
        self.All_dfs["SSIntShip".lower()] = SSIntShip

    def splittingOrdersTables(self):
        filter_list = self.All_dfs["rcextship"]["ExtShip_id"]
        self.All_dfs["rcextorders"] = self.All_dfs["externalorders"][
            self.All_dfs["externalorders"].ExtShip_id.isin(filter_list)].reset_index(drop=True)
        filter_list = self.All_dfs["scextship"]["ExtShip_id"]
        self.All_dfs["scextorders"] = self.All_dfs["externalorders"][
            self.All_dfs["externalorders"].ExtShip_id.isin(filter_list)].reset_index(drop=True)
        # All_dfs["scextorders"] = All_dfs["externalorders"].query("ExtShip_id.isin(@filter_list)").reset_index(drop=True)
        filter_list = self.All_dfs["srintship"]["IntShip_id"]
        self.All_dfs["srintorders"] = self.All_dfs["internalorders"][
            self.All_dfs["internalorders"].IntShip_id.isin(filter_list)].reset_index(drop=True)
        # All_dfs["srintorders"] = All_dfs["internalorders"].query("IntShip_id.isin(@filter_list)").reset_index(drop=True)
        filter_list = self.All_dfs["ssintship"]["IntShip_id"]
        self.All_dfs["ssintorders"] = self.All_dfs["internalorders"][
            self.All_dfs["internalorders"].IntShip_id.isin(filter_list)].reset_index(drop=True)

    def removeRedundantTables(self):
        self.All_dfs.pop("internalshipments")
        self.All_dfs.pop("externalshipments")
        self.All_dfs.pop("externalorders")
        self.All_dfs.pop("internalorders")

    def removeNans(self):
        for table in self.All_dfs:
            self.All_dfs[table].fillna('Unknown', inplace=True)

    def __add_products_to_ssintorders(self):

        products_df = self.All_dfs['products']

        factory_ids = self.All_dfs["manufacturing"]['Factory_id']
        manufactured_products = self.All_dfs["manufacturing"]["Product_id"]
        product_ids = pd.Series(self.All_dfs["products"]["prod_id"].unique())

        not_manufactured_products = product_ids[~product_ids.isin(manufactured_products)].unique()

        products_column = []
        order_cost = []
        for _, row in self.All_dfs["ssintorders"].iterrows():
            ship = row["IntShip_id"]
            quantity = row['quantity']
            sup_id = self.All_dfs["ssintship"].query(f'IntShip_id == {ship}')["listSuppIds"].iloc[0]

            intersection = np.where(factory_ids == sup_id)[0]
            product = ""
            if (len(intersection) >= 1):
                product = manufactured_products[random.choice(intersection)]
                # products_column.append(manufactured_products[random.choice(intersection)])
            elif (len(intersection) == 0):
                product = random.choice(not_manufactured_products)
                # products_column.append(random.choice(not_manufactured_products))
            prod_price = products_df[products_df["prod_id"] == product].iloc[0]['price']

            new_prod_price = prod_price * quantity

            products_column.append(product)
            order_cost.append(new_prod_price)

        self.All_dfs['ssintorders']["prod_id"] = products_column
        self.All_dfs['ssintorders']["cost"] = order_cost


    def __add_products_to_internal_orders(self):

        def add_products(pre):
            internal_orders_df = self.All_dfs[f"{pre}intorders"]
            internal_shipments_df = self.All_dfs[f"{pre}intship"]

            internal_orders_df_numpy = internal_orders_df.to_numpy()

            columns_index = dict()

            for index, column_name in enumerate(internal_orders_df.columns):
                columns_index[column_name] = index

            products_df = self.All_dfs['products']
            factory_ids = self.All_dfs["manufacturing"]['Factory_id']
            manufactured_products = self.All_dfs["manufacturing"]["Product_id"]
            product_ids = pd.Series(self.All_dfs["products"]["prod_id"].unique())

            not_manufactured_products = product_ids[~product_ids.isin(manufactured_products)].unique()

            products_column = []
            order_cost = []
            for row in internal_orders_df_numpy:
                ship = row[columns_index["IntShip_id"]]
                quantity = row[columns_index['quantity']]
                ship_row = internal_shipments_df.query(f'IntShip_id == {ship}')
                sup_id = ship_row["listSuppIds"].iloc[0]

                intersection = np.where(factory_ids == sup_id)[0]

                product = ""

                if len(intersection) >= 1:
                    product = manufactured_products[random.choice(intersection)]
                    # products_column.append(manufactured_products[random.choice(intersection)])
                elif len(intersection) == 0:
                    product = random.choice(not_manufactured_products)
                    # products_column.append(random.choice(not_manufactured_products))
                prod_price = products_df[products_df["prod_id"] == product].iloc[0]['price']

                new_prod_price = prod_price * quantity

                products_column.append(product)
                order_cost.append(new_prod_price)

            internal_orders_df["prod_id"] = products_column
            internal_orders_df["cost"] = order_cost
            self.All_dfs[f"{pre}intorders"] = internal_orders_df

        add_products("ss")
        add_products("sr")


    def __add_products_to_srintorders(self):

        internal_orders_df = self.All_dfs["srintorders"]
        internal_shipments_df = self.All_dfs["srintship"]

        internal_orders_df_numpy = internal_orders_df.to_numpy()

        columns_index = dict()

        for index, column_name in enumerate(internal_orders_df.columns):
            columns_index[column_name] = index

        products_df = self.All_dfs['products']
        factory_ids = self.All_dfs["manufacturing"]['Factory_id']
        manufactured_products = self.All_dfs["manufacturing"]["Product_id"]
        product_ids = pd.Series(self.All_dfs["products"]["prod_id"].unique())

        not_manufactured_products = product_ids[~product_ids.isin(manufactured_products)].unique()

        products_column = []
        order_cost = []
        for row in internal_orders_df_numpy:
            ship = row[columns_index["IntShip_id"]]
            quantity = row[columns_index['quantity']]
            ship_row = internal_shipments_df.query(f'IntShip_id == {ship}')
            sup_id = ship_row["listSuppIds"].iloc[0]

            intersection = np.where(factory_ids == sup_id)[0]

            product = ""

            if len(intersection) >= 1:
                product = manufactured_products[random.choice(intersection)]
                # products_column.append(manufactured_products[random.choice(intersection)])
            elif len(intersection) == 0:
                product = random.choice(not_manufactured_products)
                # products_column.append(random.choice(not_manufactured_products))
            prod_price = products_df[products_df["prod_id"] == product].iloc[0]['price']

            new_prod_price = prod_price * quantity

            products_column.append(product)
            order_cost.append(new_prod_price)

        internal_orders_df["prod_id"] = products_column
        internal_orders_df["cost"] = order_cost
        self.All_dfs["srintorders"] = internal_orders_df

    def __add_products_to_scextorders(self):

        products_df = self.All_dfs['products']
        factory_ids = self.All_dfs["manufacturing"]['Factory_id']
        manufactured_products = self.All_dfs["manufacturing"]["Product_id"]
        product_ids = pd.Series(self.All_dfs["products"]["prod_id"].unique())

        not_manufactured_products = product_ids[~product_ids.isin(manufactured_products)].unique()

        products_column = []
        order_cost = []
        for _, row in self.All_dfs["scextorders"].iterrows():
            ship = row["ExtShip_id"]
            quantity = row['quantity']
            ship_row = self.All_dfs["scextship"].query(f'ExtShip_id == {ship}')
            sup_id = ship_row["factoryIds/retailerIds"].iloc[0]
            # ret_id = ship_row["factoryIds"].iloc[0]
            intersection = np.where(factory_ids == sup_id)[0]
            product = ""
            if (len(intersection) >= 1):
                product = manufactured_products[random.choice(intersection)]
                # products_column.append(manufactured_products[random.choice(intersection)])
            elif (len(intersection) == 0):
                product = random.choice(not_manufactured_products)
                # products_column.append(random.choice(not_manufactured_products))
            prod_price = products_df[products_df["prod_id"] == product].iloc[0]['price']

            new_prod_price = prod_price * quantity

            products_column.append(product)
            order_cost.append(new_prod_price)

        self.All_dfs['scextorders']["prod_id"] = products_column
        self.All_dfs['scextorders']["cost"] = order_cost

    def __add_products_to_rcextorders(self):

        products_df = self.All_dfs['products']
        retailer_df = self.All_dfs['retailer']
        rcextorders_df = self.All_dfs['rcextorders']
        rcextship_df = self.All_dfs['rcextship']

        manufactured_products = self.All_dfs["manufacturing"]["Product_id"]
        product_ids = pd.Series(self.All_dfs["products"]["prod_id"].unique())

        not_manufactured_products = product_ids[~product_ids.isin(manufactured_products)].unique()

        columns_index = dict()

        print(retailer_df.columns)

        for index, column_name in enumerate(rcextorders_df.columns):
            columns_index[column_name] = index

        rcextorders_df_numpy = rcextorders_df.to_numpy()

        products_column = []
        order_cost = []

        for row in rcextorders_df_numpy:
            ship = row[columns_index["ExtShip_id"]]
            quantity = row[columns_index['quantity']]
            ship_row = rcextship_df.query(f'ExtShip_id == {ship}')
            retailer_id = ship_row["factoryIds/retailerIds"].iloc[0]
            # ret_id = ship_row["factoryIds"].iloc[0]
            retailer_products = retailer_df.query(f"retailer_id == {retailer_id}").iloc[0]["products_sold"]
            product = ""
            if len(retailer_products) >= 1:
                product = random.choice(retailer_products)

            elif len(retailer_products) == 0:
                product = random.choice(not_manufactured_products)

            prod_price = products_df[products_df["prod_id"] == product].iloc[0]['price']

            new_prod_price = prod_price * quantity

            products_column.append(product)
            order_cost.append(new_prod_price)

        rcextorders_df["prod_id"] = products_column
        rcextorders_df["cost"] = order_cost
        self.All_dfs["rcextorders"] = rcextorders_df

    def __add_type_and_products_sold_to_retailer(self):
        """
        Add type and products sold to retailer

        :return: NONE
        """

        supplier_to_retailer_shipment_df = self.All_dfs["srintship"]
        retailer_df = self.All_dfs["retailer"]
        supplier_df = self.All_dfs["supplier"]
        srorders_df = self.All_dfs["srintorders"]
        suppliers_ids_in_shipments = list(supplier_to_retailer_shipment_df.listSuppIds.unique())
        suppliers_in_shipments_df = supplier_df[supplier_df["supp_id"].isin(suppliers_ids_in_shipments)]
        retailers_types_column = []
        products_sold_column = []
        for retailer_row in retailer_df.to_dict('records'):
            retailer_types = set()
            retailer_id = retailer_row["retailer_id"]
            products_sold = set()
            shipments_with_retailer_df = supplier_to_retailer_shipment_df[
                supplier_to_retailer_shipment_df.factoryIds == retailer_id]
            for shipment_row in shipments_with_retailer_df.to_dict('records'):
                supplier_id = shipment_row["listSuppIds"]
                ship_id = shipment_row["IntShip_id"]
                supplier_type = suppliers_in_shipments_df[suppliers_in_shipments_df.supp_id == supplier_id].iloc[0].type
                retailer_types.add(supplier_type)

                retailer_orders_numpy = srorders_df.query(f"IntShip_id == {ship_id}").to_numpy()

                for row in retailer_orders_numpy:
                    products_sold.add(row[-1])

            retailers_types_column.append(list(retailer_types))
            products_sold_column.append(list(products_sold))

        retailer_df["products_sold"] = products_sold_column
        retailer_df["retailer_types"] = retailers_types_column
        cols = retailer_df.columns.tolist()
        cols = cols[:5] + cols[-1:] + cols[5:-1]
        self.All_dfs["retailer"] = retailer_df[cols]

    def __add_manufacturing_price(self):
        """
        Compute manufacturing cost for each manufacturer (supplier)
        :return: NONE
        """

        new_column = []
        manf_df = self.All_dfs["manufacturing"]
        products_df = self.All_dfs["products"]

        for _, row in manf_df.iterrows():
            product_id = row["Product_id"]
            prod_price = products_df[products_df["prod_id"] == product_id].iloc[0]['price']
            manf_cost = int(prod_price * random.uniform(0.2, 0.9) * 100) / 100
            new_column.append(manf_cost)
        self.All_dfs["manufacturing"]["ManufacturingCost"] = new_column
