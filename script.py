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
        # Get column names from the first row of the CSV
        column_names = ['id','rut','dv','nombre','sexo','direccion','comuna','region','pension_1','pension_2','ley_de_quiebra','neitcom']
        # REG	    RUT	    dv	APELLIDOSNOMBRES	SEXO	DIRECCION	ID_COMUNA	ID_REGION	PENSION_1	    PENSION_2	                ley_quiebra	neitcom
        # 128851	2924468	5	ZUnIGA HERMOSILLA ISOLINA DEL CARMEN	FEMENINO	PB 11 DE SEPT E RIQUELME 464	150	8	IPS	NULL	0	        0
        # 128852	2924479	0	LAFQUEN ACUM CARLOS	MASCULINO	VICENTE REYES 222	247	10	IPS	NULL	0	0
        
        paraquenotomelaprimera = next(reader)
        column_names_str = ', '.join(column_names)
        for row in reader:
   
            # fila = [row.replace("'",'')]
            valores = ",".join("'" + x.replace("'",'') + "'" for x in row)
            
            query = "INSERT INTO {} ({}) VALUES ({})".format(table_name, column_names_str, valores)
            print(row)
            print(query)
         
         
            cursor.execute(query)
           

# Commit the changes and close the cursor and connection
cnx.commit()
cursor.close()
cnx.close()