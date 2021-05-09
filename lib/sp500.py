from datapackage import Package
import mysql.connector


# db conn
cnx = mysql.connector.connect(user='root',
                                  password='root',
                                  host='127.0.0.1',
                                  database='fin',
                                  auth_plugin='mysql_native_password')

c = cnx.cursor()
package = Package('https://datahub.io/core/s-and-p-500-companies/datapackage.json')

# print list of all resources:
#print(package.resource_names)

sp = list()
# print processed tabular data (if exists any)
for resource in package.resources:
    if resource.descriptor['datahub']['type'] == 'derived/csv':
        sp.append(resource.read())

final_sp = list()
for cpm in sp:
    for number in cpm:
        final_sp.append(number)
        print(number)
    
        sql = "INSERT INTO sp500(ticker,company_name,sector) VALUES (%s, %s, %s)"
        val = (number[0], number[1],number[2])
        c.execute(sql, val)

        cnx.commit()
