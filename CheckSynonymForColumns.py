import wordninja

# This class is to convert any new column names in the table to our column naming convention using synonyms
class ConvertColumnsUsingSynonym:
    def __init__(self, All_dfs, tableName, nlp):
        self.All_dfs = All_dfs
        self.tableName = tableName
        self.nlp = nlp
        self.convertedColumn = []
        self.originalTablesWithColumns = {
        'customer': ['cust_id', 'gender', 'first_name', 'last_name', 'country', 'nationality', 'profession', 'marital_status', 'education_level', 'age'], 
        'externalservices': ['ExtServ_id', 'ExtTrans_id', 'placed_when', 'actual_date', 'expected_date', 'quota', 'status'], 
        'externaltransactions': ['ExtTran_id', 'CompFrom', 'Custto'], 
        'facilities': ['fac_id', 'supplier_id', 'market_cap', 'country', 'city_name', 'currency', 'number_of_employees', 'Annual_sales'], 
        'internalservices': ['IntServ_id', 'IntTrans_id', 'placed_when', 'actual_date', 'expected_date', 'quota', 'status'], 
        'internaltransactions': ['IntTran_id', 'CompFrom', 'Compto'], 
        'manufacturing': ['Manf_id', 'Different_suppliers', 'Product_id', 'Factory_id', 'yield (%)', 'target (%)'], 
        'products': ['prod_id', 'product_type', 'product_name', 'warehouses', 'price', 'profit_margin (%)'], 
        'retailer': ['retailer_id', 'country', 'city_name', 'type', 'reviews_number', 'rating', 'opening_hours', 'capacity (units)'], 
        'supplier': ['supp_id', 'supplier_name', 'country', 'city_name', 'currency', 'type', 'number_of_employees', 'Annual_sales', 'number_of_orders', 'market_share (%)', 'market_capitalization ($)'], 
        'warehouses': ['warehouse_id', 'country', 'city_name', 'capacity (NA)', 'product_types', 'Operations Expenses', 'Rental price'], 
        'externalshipments': ['ExtShip_id', 'factoryIds/retailerIds', 'idsTo', 'from_to_where', 'TransportationType'], 
        'internalshipments': ['IntShip_id', 'listSuppIds', 'factoryIds', 'from_to_where', 'TransportationType'], 
        'externalorders': ['ExtOrders_id', 'ExtShip_id', 'quantity', 'placed_when', 'actual_date', 'expected_date', 'cost', 'status'], 
        'internalorders': ['IntOrders_id', 'IntShip_id', 'quantity', 'placed_when', 'actual_date', 'expected_date', 'cost', 'status']}
        self.convertNewColumns()

    # Loops over our original column names and the new column names and compare the two lists and convert using synonyms
    def convertNewColumns(self):
        originalColumns = self.originalTablesWithColumns[self.tableName]
        newColumns = list(self.All_dfs[self.tableName].columns)
        for i in range(len(originalColumns)):
            storePercentages = {}
            newKey = ''
            indexToRemove = 0
            for j in range(len(newColumns)):
                book1_topics = [' '.join(wordninja.split(str(originalColumns[i]).lower()))]
                book2_topics = [' '.join(wordninja.split(str(newColumns[j]).lower()))]
                doc1 = self.nlp(' '.join(book1_topics))
                doc2 = self.nlp(' '.join(book2_topics))
                storePercentages[newColumns[j]] = doc1.similarity(doc2) 
            storePercentages = {k: v for k, v in sorted(storePercentages.items(), key=lambda item: item[1])}
            newKey = list(storePercentages.keys())[-1]
            indexToRemove = newColumns.index(newKey)
            del newColumns[indexToRemove]
            newColumns.insert(i, originalColumns[i])
        self.convertedColumn = newColumns


