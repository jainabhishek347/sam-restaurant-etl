import csv
import psycopg2
import requests

class trips_config:
    global conn,cur
    conn = psycopg2.connect(dbname = 'amandb', password = 12345, port = 5432, user = 'aman', host = 'localhost')
    cur = conn.cursor()
    
    def __init__(self,url):
        self.url=url
        self.csv_config_file = cur.execute("select config_file_path FROM trips_config;")
        self.csv_config_file = cur.fetchone()[0]

        self.source_table_name = cur.execute("select source_table_name FROM trips_config;")
        self.source_table_name = cur.fetchone()[0]

        self.target_table_name = cur.execute("select target_table_name FROM trips_config;")
        self.target_table_name = cur.fetchone()[0]
        
        self.output_file_path = cur.execute("select output_file_path FROM trips_config;")
        self.output_file_path = cur.fetchone()[0]

        self.output_file_name = cur.execute("select output_file_name FROM trips_config;")
        self.output_file_name = cur.fetchone()[0]

        self.target_alias = cur.execute("select target_alias FROM trips_config;")
        self.target_alias = cur.fetchone()[0]

        self.source_alias = cur.execute("select source_alias FROM trips_config;")
        self.source_alias = cur.fetchone()[0]
        
        self.final_alias = cur.execute("select final_alias FROM trips_config;")
        self.final_alias = cur.fetchone()[0]

        self.final_table_name = cur.execute("select final_table_name FROM trips_config;")
        self.final_table_name = cur.fetchone()[0]
        
        # getting data from csv
        self.source_dtypes =[]
        self.target_dtypes = []
        self.source_column_names = []
        self.target_column_names = []
        self.dim_column_values = []
        self.final_column_values = []
        self.final_column_names = []
        with open(self.csv_config_file, "r") as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if row['source_column_names'] != "" : self.source_column_names.append(row['source_column_names']) 
                if row['target_column_names'] != "" : self.target_column_names.append(row['target_column_names'])
                if row['target_dtype'] != "" : self.target_dtypes.append(row['target_dtype'])
                if row['source_dtype'] != "" : self.source_dtypes.append(row['source_dtype'])
                if row['dim_column_values'] != "" : self.dim_column_values.append(row['dim_column_values'])
                if row['final_column_values'] != "" : self.final_column_values.append(row['final_column_values'])
                if row['final_column_names'] != "" : self.final_column_names.append(row['final_column_names'])
        
        self.insert_trips_stage()

    def insert_trips_stage(self):
        try:
            # getting data and header from api 
            url_data = requests.get(self.url).text
            csv_data = csv.reader(url_data.splitlines())

            # creating query for creating stage table 
            create_stage_table_query = "CREATE TABLE IF NOT EXISTS {} (".format(self.source_table_name)
            for col_name in self.source_column_names:
                create_stage_table_query += "{} TEXT, ".format(col_name)
            create_stage_table_query = create_stage_table_query.rstrip(", ")+");"
            cur.execute(create_stage_table_query)
            
            # copy data from csv to stage table
            with open(self.output_file_name, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(csv_data)
            cur.execute(f"COPY {self.source_table_name} FROM '{self.output_file_path}{self.output_file_name}' DELIMITER ',' CSV HEADER;")                
            print("TRIPS_STAGE_TABLE created and data is copied to TRIPS_STAGE_TABLE")
            conn.commit()
            self.insert_trips_stage_toDim()
        except Exception as e:
            print(e)
            conn.rollback()
        
    def insert_trips_stage_toDim(self):
        try:
            create_dim_table_query = "CREATE TABLE IF NOT EXISTS {} (".format(self.target_table_name)
            for col_name,tar_dtypes in zip(self.dim_column_values,self.target_dtypes):
                create_dim_table_query += "{} {}, ".format(col_name.split(",")[0],tar_dtypes)           
            create_dim_table_query = create_dim_table_query.rstrip(", ")+");"
            cur.execute(create_dim_table_query)
            print("TRIPS_DIM_TABLE is created")
            
            insert_dim_query = f"INSERT INTO {self.target_table_name} ("
            for column in self.dim_column_values:
                insert_dim_query+= f"{column.split(',')[0]}, "
            insert_dim_query = insert_dim_query.rstrip(", ")+")\nSELECT  "
            for indx,values in enumerate(self.dim_column_values):
                if values.split(",")[0] == 'updated' or values.split(",")[0] == 'created' or values.split(",")[0] == 'is_deleted' or values.split(",")[0] == 'is_active':
                    insert_dim_query += values.split(",")[1]+', ' 
                else:
                    insert_dim_query += f"CAST({self.source_alias}.{str(values.split(',')[1])} AS {str(self.target_dtypes[indx])}),"
            insert_dim_query = insert_dim_query.rstrip(", ")+f"\nFROM {self.source_table_name} {self.source_alias} WHERE NOT EXISTS ( SELECT 1 FROM {self.target_table_name} {self.target_alias} WHERE {self.target_alias}.{self.target_column_names[0]} = {self.source_alias}.{self.source_column_names[0]});"
            cur.execute(insert_dim_query)           
            print("Inserting Unique records in TRIPS_DIM_TABLE")

            insert_onlyUpdated_dim_query = f"INSERT INTO {self.target_table_name} ("
            for col_name in self.dim_column_values:
                insert_onlyUpdated_dim_query += f"{col_name.split(',')[0]}, "
            insert_onlyUpdated_dim_query = insert_onlyUpdated_dim_query.rstrip(", ")+")\nSELECT "
            for indx,values in enumerate(self.dim_column_values):
                if values.split(",")[0] == 'updated' or values.split(",")[0] == 'created' or values.split(",")[0] == 'is_deleted' or values.split(",")[0] == 'is_active':
                    insert_onlyUpdated_dim_query += values.split(",")[1]+', ' 
                else:
                    insert_onlyUpdated_dim_query += f"CAST({self.source_alias}.{str(values.split(',')[1])} AS {str(self.target_dtypes[indx])}), "
            insert_onlyUpdated_dim_query = insert_onlyUpdated_dim_query.rstrip(", ")+f"\nFROM {self.source_table_name} {self.source_alias} \nLEFT JOIN {self.target_table_name} {self.target_alias} ON {self.target_alias}.{self.target_column_names[0]} = {self.source_alias}.{self.source_column_names[0]} WHERE {self.target_alias}.{self.target_column_names[0]} = {self.source_alias}.{self.source_column_names[0]} AND\n("
            for s_colname,t_colname in zip(self.target_column_names,self.dim_column_values):
                if s_colname == 'trip_id' or s_colname == 'updated' or s_colname == 'created' or s_colname == 'is_deleted' or s_colname == 'is_active':
                    pass
                else:
                    insert_onlyUpdated_dim_query+= f"CAST({self.source_alias}.{str(t_colname.split(',')[1])} AS {str(self.target_dtypes[self.target_column_names.index(s_colname)])})<>{self.target_alias}.{t_colname.split(',')[0]} OR "
            insert_onlyUpdated_dim_query = insert_onlyUpdated_dim_query.rstrip("OR ")+f")\nAND {self.target_alias}.updated >= (SELECT MAX(created) FROM {self.target_table_name} WHERE {self.target_alias}.{self.target_column_names[0]} = {self.target_table_name}.{self.target_column_names[0]}) AND is_active = 1;"       
            cur.execute(insert_onlyUpdated_dim_query)
            print("Inserting records with same id but with different values in TRIPS_DIM_TABLE")
            
            update_is_activeAnd_updatedQuery = f"""UPDATE {self.target_table_name}
                    SET is_active = 0, updated = now()
                    FROM {self.source_table_name} {self.source_alias}
                    WHERE {self.source_alias}.{self.target_column_names[0]} = {self.target_table_name}.{self.target_column_names[0]} AND ("""
            for s_colname,t_colname in zip(self.target_column_names,self.dim_column_values):
                if s_colname == 'trip_id' or s_colname == 'updated' or s_colname == 'created' or s_colname == 'is_deleted' or s_colname == 'is_active':
                    pass
                else:
                    update_is_activeAnd_updatedQuery+= f"CAST({self.source_alias}.{str(t_colname.split(',')[1])} AS {str(self.target_dtypes[self.target_column_names.index(s_colname)])})<>{self.target_table_name}.{t_colname.split(',')[0]} OR "
            update_is_activeAnd_updatedQuery = update_is_activeAnd_updatedQuery.rstrip('OR ')+') AND is_active = 1;'   
            cur.execute(update_is_activeAnd_updatedQuery)
            print("Status for is_active and Updated time is now set")

            update_is_deleted_query = f"""UPDATE {self.target_table_name}\n SET is_active = 0, is_deleted =1, updated = now()
            WHERE NOT EXISTS (SELECT 1 FROM {self.source_table_name} WHERE {self.source_table_name}.{self.target_column_names[0]} = {self.target_table_name}.{self.target_column_names[0]});
            """
            cur.execute(update_is_deleted_query)
            print("Status for is_deleted,is_active and Updated time is now set")
            conn.commit()
            print("Dim table loaded with new values and existing are updated if there is any.........")
            self.insert_trips_stage_tofinal()
        except Exception as err:
            print(err)
            conn.rollback()

    def insert_trips_stage_tofinal(self):
        try:
            create_final_table_query = "CREATE TABLE IF NOT EXISTS {} (".format(self.final_table_name)
            for col_name,tar_dtypes in zip(self.final_column_values,self.target_dtypes):
                create_final_table_query += "{} {}, ".format(col_name.split(",")[0],tar_dtypes)           
            create_final_table_query = create_final_table_query.rstrip(", ")+");"
            cur.execute(create_final_table_query)
            print("TRIPS_FINAL_Table is created")

            insert_final_query = f"INSERT INTO {self.final_table_name} ("
            for column in self.dim_column_values:
                insert_final_query+= f"{column.split(',')[0]}, "
            insert_final_query = insert_final_query.rstrip(", ")+")\nSELECT  "

            for indx,values in enumerate(self.final_column_values):
                if values.split(",")[0] == 'updated' or values.split(",")[0] == 'created' or values.split(",")[0] == 'is_deleted' or values.split(",")[0] == 'is_active':
                    insert_final_query += values.split(",")[1]+', ' 
                else:
                    insert_final_query += f"CAST({self.source_alias}.{str(values.split(',')[1])} AS {str(self.target_dtypes[indx])}),"
            insert_final_query = insert_final_query.rstrip(", ")+f"\nFROM {self.source_table_name} {self.source_alias} \nLEFT JOIN {self.final_table_name} {self.final_alias} \nON {self.source_alias}.{self.final_column_names[0]} = {self.final_alias}.{self.final_column_names[0]} WHERE {self.final_alias}.{self.final_column_names[0]} IS NULL AND NOT EXISTS (SELECT 1 FROM {self.final_table_name} WHERE {self.final_table_name}.{self.final_column_names[0]} = {self.source_alias}.{self.source_column_names[0]});"  
            cur.execute(insert_final_query)
            print("Unique records are inserted into TRIPS_FINAL_TABLE")

            update_final_values = f"""UPDATE {self.final_table_name}\nSET updated = now(), is_active = 1, """
            for indx,val in enumerate(self.final_column_values):
                if val.split(',')[0] == 'trip_id' or val.split(',')[0] == 'created' or val.split(',')[0] == 'updated' or val.split(',')[0] == 'is_active' or val.split(',')[0] == 'is_deleted':
                    pass
                else:
                    update_final_values += f"{val.split(',')[0]} = CAST({self.source_alias}.{str(val.split(',')[1])} AS {str(self.target_dtypes[indx])}), "
            update_final_values = update_final_values.rstrip(', ')+f' \nFROM {self.source_table_name} {self.source_alias} WHERE {self.source_alias}.{self.source_column_names[0]} = {self.final_table_name}.{self.target_column_names[0]} \nAND ('
            for indx,values in enumerate(self.final_column_values):
                if values.split(",")[0] == 'trip_id' or values.split(",")[0] == 'updated' or values.split(",")[0] == 'created' or values.split(",")[0] == 'is_deleted' or values.split(",")[0] == 'is_active':
                    pass
                else:
                    update_final_values += f"CAST({self.source_alias}.{str(values.split(',')[1])} AS {str(self.target_dtypes[indx])}) <> {self.final_table_name}.{values.split(',')[0]} OR "
            update_final_values = update_final_values.rstrip('OR ')+");"
            cur.execute(update_final_values)
            print("Records are updated if there is any changes in old records ")

            update_is_deleted = f""" UPDATE {self.final_table_name} 
            SET is_active = 0, is_deleted = 1, updated = now()
            WHERE NOT EXISTS (SELECT 1 FROM {self.source_table_name} WHERE {self.final_table_name}.{self.target_column_names[0]} = {self.source_table_name}.{self.target_column_names[0]});
            """
            cur.execute(update_is_deleted)
            print("Status for is_deleted,is_active and Updated time is now set")

            truncate_stage_query = f"""TRUNCATE TABLE {self.source_table_name}"""
            cur.execute(truncate_stage_query)

            print("TRIPS_STAGE_TABLE is now Empty")
            print("______________________________________JOB-DONE___________________________________________")
            conn.commit()
        except Exception as err:
            print(err)
            conn.rollback()
my_obj = trips_config('https://data.cityofchicago.org/resource/ukxa-jdd7.csv')
conn.close()
cur.close()

