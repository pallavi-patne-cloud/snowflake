import snowflake.connector

class SnowflakeConnector:
    def __init__(self,account,user,password,warehouse,database,schema,role):
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
    def connect(self):
        self.connection = snowflake.connector.connect(
            user = self.user,
            password = self.password,
            account = self.account,
            warehouse = self.warehouse,
            database = self.database,
            schema = self.schema   

        )
        self.cursor = self.connection.cursor()

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def close_connection(self):
        self.cursor.close()
        self.connection.close()

###  Below details must be in config and password in hashicorp vault before commiting to git
# account = "TNWYUNB-GF82581"
# user = "PALLAVI"
# password = "Ayaan2012"
# database = "SNOWFLAKE_SAMPLE_DATA"
# schema = "TPCH_SF1"
# role = "SYSADMIN"
# warehouse = "COMPUTE_WH"

account = "TNWYUNB-GF82581"
user = "PALLAVI"
password = "Ayaan2012"
database = "SNOWFLAKE_SAMPLE_DATA"
schema = "TPCH_SF1"
role = "SYSADMIN"
warehouse = "COMPUTE_WH"

sf_connector = SnowflakeConnector(
    account,user,password,warehouse,database,schema,role

)
sf_connector.connect()

query = "select * from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER LIMIT 10"
result = sf_connector.execute_query(query)
print(result)

sf_connector.close_connection()
