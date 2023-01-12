import mysql.connector
import csv

# Connect to MySQL database
cnx = mysql.connector.connect(user='root', password='', host='localhost', database='python_scripts')
cursor = cnx.cursor()

# Define the table name
table_name = "validacion_externa"

# List of CSV files to import
file_list = ['example.csv']

# Iterate over the files and import them into the table
for file in file_list:
    with open(file, 'r') as f:
        reader = csv.reader(f,delimiter=";")
        
        # campos de validacion externa
        column_names = ['id','rut','dv','nombre','sexo','direccion','comuna','region','pension_1','pension_2','ley_de_quiebra','neitcom']
        # obvia la primera columna
        paraquenotomelaprimera = next(reader)
        # coloca los nombres en comas para el insert
        column_names_str = ', '.join(column_names)
        for row in reader:
   
            # crea el string con sus values
            valores = ",".join("'" + x.replace("'",'') + "'" for x in row)
            # commet
            query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, column_names_str, valores)
            print(row)
            print(query)
         
         
            cursor.execute(query)
           

# Commit the changes and close the cursor and connection
cnx.commit()
cursor.close()
cnx.close()