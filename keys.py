class keys:


    def __init__(self, All_dfs):
        self.All_dfs = All_dfs
        self.pk = {}
        self.fk = {}
        self.ref_in = {}
        self.fkandrefinSetter()


    # find each table's primary key
    def pk_finder(self,name, df):
        for columnNumber in range(len(df.columns)):
            col = df.iloc[:,columnNumber]
        #if(col.size == col.drop_duplicates().size):
            if(col.nunique() == col.size):
                self.pk[name] = df.columns[columnNumber] 
                break
    
    # find each table's foreign key

    def fk_finder(self,tableName, df):
        for columnNumber in range(len(df.columns)):
            col_name = df.columns[columnNumber]
            col = df[col_name]
            for pkTable in self.pk: #loops on the key j = table names
                if pkTable != tableName:
                    primary_key_column = self.All_dfs[pkTable][(self.pk[pkTable])]
                    if primary_key_column.dtype == col.dtype:
                        isReferenced = col.isin(primary_key_column) #what is the status
                        if isReferenced[isReferenced == False].size == 0:
                            self.fk[tableName][col_name] = pkTable
                            self.ref_in[pkTable].add(tableName)
                            break
                    elif (isinstance(col[0],list) and type(primary_key_column[0]) == type(col[0][0])):
                        isReferenced = col.explode().reset_index(drop=True).isin(primary_key_column)
                        if isReferenced[isReferenced == False].size == 0:
                            self.fk[tableName][col_name] = pkTable
                            self.ref_in[pkTable].add(tableName)
                            break

    #Initialize fk and refin
    def fkandrefinSetter(self):
        table_name = list(self.All_dfs.keys())
        for table_name in self.All_dfs:
            self.fk[table_name] = {}
            self.ref_in[table_name] = set()


   
    # Primary key Getter 
    def primaryKeyGetter(self):
        for t in self.All_dfs:
            self.pk_finder(t,self.All_dfs[t])
        
        return self.pk
    
    # Foreign key Getter
    def foreignKeyGetter(self):
        for t in self.All_dfs:
            self.fk_finder(t,self.All_dfs[t])
        return self.fk
