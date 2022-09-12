import os
import pandas as pd
import random
import numpy as np
from functools import reduce

class ReadingDataSet:

    def __init__(self):
        # self.path = path
        self.All_dfs = {}
        self.createDataframes()
        self.removeNans()
        self.handleNonAtomicColumns()
        self.createMissingShipments()
        self.createMissingOrders()
        self.splittingShipmentsTables()
        self.splittingOrdersTables()
        self.removeRedundantTables()
        self.addProductsToOrders()


    def createDataframes(self):
        path_of_the_directory = './DataSet/'
        ext = ('.csv')
        for file in os.listdir(path_of_the_directory):
            if file.endswith(ext):
                print(file) 
                temp = (file.replace("_"," ").replace("."," ").split(" ")[0].lower())
                self.All_dfs[temp] = pd.read_csv(path_of_the_directory+file)
            else:
                continue

    def and_agg(self, series):
       return reduce(lambda x, y: x and y, series)

    def handleNonAtomicColumns(self):
        for table_nm in self.All_dfs:
            table = self.All_dfs[table_nm]
            for column_name in table.columns:
                column =  table[column_name]
                if column.dtype=='O' and isinstance(column[0],str):
                    if  (isinstance(column[0],str) and column.apply(lambda x: (str(x).startswith('[') and str(x).endswith(']'))
                                or 
                                (str(x).startswith('(') and str(x).endswith(')'))).agg(self.and_agg)):
                        
                        s = column.apply(lambda x: x.strip("[](,)").split(','))
                        if (s[0][0].isdigit()):
                            s = s.apply(lambda x: list(map(np.int64,x)))
                        self.All_dfs[table_nm][column_name] = s
    
    def createMissingShipments(self):
        c = 9995

        for r in range(len(self.All_dfs["manufacturing"])):
            diff_supp = self.All_dfs["manufacturing"].loc[r,"Different_suppliers"]
            factory = self.All_dfs["manufacturing"].loc[r,"Factory_id"]
            for s in range(len(diff_supp)):
                
                supp = diff_supp[s]
                if supp == factory:
                    print(factory)
                if len(self.All_dfs["internalshipments"].query(f"listSuppIds == {supp} and factoryIds == {factory}")) ==0:
                    new_row = [{"IntShip_id":c, "listSuppIds":supp, "factoryIds": factory, "from_to_where": "SS"}] 
                    df = pd.DataFrame(new_row)
                    c = c+1
                    self.All_dfs["internalshipments"] = pd.concat([self.All_dfs["internalshipments"], df], ignore_index=True)
        
    def createMissingOrders(self):
        orders_int_ship_id = self.All_dfs["internalorders"].IntShip_id	
        internal_ship_id = self.All_dfs["internalshipments"].IntShip_id
        ship_id_in_orders_ship_id = internal_ship_id.isin(orders_int_ship_id)
        ship_id_not_in_orders_ship_id = list(self.All_dfs["internalshipments"].IntShip_id[[not elem for elem in ship_id_in_orders_ship_id]].unique())

        c = 9931
        for id in ship_id_not_in_orders_ship_id:
            new_row = [{"IntOrders_id":c, "IntShip_id":id, "quantity": 766, "placed_when": "1993-05-08",
            "actual_date":"1993-05-30 00:00:00", "expected_date":"1993-06-01", "cost":"38.90", "status":"Closed"}] 
            df = pd.DataFrame(new_row)
            c = c+1
            self.All_dfs["internalorders"] = pd.concat([self.All_dfs["internalorders"], df], ignore_index=True)

    def splittingShipmentsTables(self):
        SRIntShip = self.All_dfs["internalshipments"].query('from_to_where == "SR"')
        SRIntShip = SRIntShip.drop(['from_to_where'], axis=1).reset_index(drop = True)
        #SRIntShip
        RCExtShip = self.All_dfs["externalshipments"].query('from_to_where == "RC"')
        RCExtShip = RCExtShip.drop(['from_to_where'], axis=1).reset_index(drop = True)
        #RCExtShip
        SSIntShip = self.All_dfs["internalshipments"].query('from_to_where == "SS"')
        SSIntShip = SSIntShip.drop(['from_to_where'], axis=1).reset_index(drop = True)
        #SSIntShip
        SCExtShip = self.All_dfs["externalshipments"].query('from_to_where == "SC"')
        SCExtShip = SCExtShip.drop(['from_to_where'], axis=1).reset_index(drop = True)
        #SCExtShip
        self.All_dfs["RCExtShip".lower()] = RCExtShip
        self.All_dfs["SCExtShip".lower()] = SCExtShip
        self.All_dfs["SRIntShip".lower()] = SRIntShip
        self.All_dfs["SSIntShip".lower()] = SSIntShip

    def splittingOrdersTables(self):
        filter_list = self.All_dfs["rcextship"]["ExtShip_id"]
        self.All_dfs["rcextorders"] = self.All_dfs["externalorders"][self.All_dfs["externalorders"].ExtShip_id.isin(filter_list)].reset_index(drop=True)
        filter_list = self.All_dfs["scextship"]["ExtShip_id"]
        self.All_dfs["scextorders"] = self.All_dfs["externalorders"][self.All_dfs["externalorders"].ExtShip_id.isin(filter_list)].reset_index(drop=True)
        #All_dfs["scextorders"] = All_dfs["externalorders"].query("ExtShip_id.isin(@filter_list)").reset_index(drop=True)
        filter_list = self.All_dfs["srintship"]["IntShip_id"]
        self.All_dfs["srintorders"] = self.All_dfs["internalorders"][self.All_dfs["internalorders"].IntShip_id.isin(filter_list)].reset_index(drop=True)
        #All_dfs["srintorders"] = All_dfs["internalorders"].query("IntShip_id.isin(@filter_list)").reset_index(drop=True)
        filter_list = self.All_dfs["ssintship"]["IntShip_id"]
        self.All_dfs["ssintorders"] = self.All_dfs["internalorders"][self.All_dfs["internalorders"].IntShip_id.isin(filter_list)].reset_index(drop=True)

    def removeRedundantTables(self):
        self.All_dfs.pop("internalshipments")
        self.All_dfs.pop("externalshipments")
        self.All_dfs.pop("externalorders")
        self.All_dfs.pop("internalorders")

    def removeNans(self):
        for table in self.All_dfs:
            self.All_dfs[table].fillna('Unknown',inplace = True)
        

    def addProductsToOrders(self):
        x=self.All_dfs['ssintship']['listSuppIds']
        # print(len(x))
        y=self.All_dfs["manufacturing"]['Factory_id']
        # print(len(y))
        z=[]
        # print(x.isin(y).value_counts())
        # z=All_dfs['ssintorders']
        for i in range (len(x)):
            temp=np.where(y==x[i])[0]
            if(len(temp)>1):#returns the indices that makes this statement true
                z.append(random.choice(temp))
            elif(len(temp)==1):
                z.append(temp[0])
            elif(len(temp)==0):
                z.append(random.randint(0,len(y)-1))

        # now z has size = 537 and the intship has size of 548 so will randomly add 11 more values
        for i in range(11):
            z.append(random.randint(0,len(y)-1))
    
        len(z)
        prod_id=[]
        for i in range(len(z)):
            prod_id.append(self.All_dfs["manufacturing"]['Product_id'][z[i]])
        self.All_dfs['ssintorders'].insert(8,'prod_id',prod_id,True)


    