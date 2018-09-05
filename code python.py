import json
import MySQLdb as mysql
db_destination=mysql.connect(host="localhost",user="root",passwd="",db="clients")
curseur_dest = db_destination.cursor()
##################################chargement et transformation de fichier json
f_json= open("cust_data.json")
cl=json.load(f_json)
for tmp in cl:
    var1 = tmp['id']
    var2 = tmp['first_name']
    var3 = tmp['last_name']
    var4 = tmp['email']
    try:
       var5 = tmp['gender'][0] #prendre la premiere lettre uniquement
    except KeyError:# lorsque la clé valeur est manqunte  on met inconnu
        var5="Inconnu"
        pass
    var6 = tmp['ville']
    requete_insertion="insert into client_output (id , first_name , last_name ,	email ,	gender , ville)" \
        "values (%s,'%s','%s','%s','%s','%s')" % ( var1, var2, var3, var4, var5, var6)
    curseur_dest.execute(requete_insertion)
##################################### chargement  et transformation de fichier csv
f = open("week_cust.csv")
next(f)
for tmp in f:
    var1, var2, var3, var4, var5, var6 = tmp.split(',')
    if str.strip(var6) == '':#transformer les villes qui sont vides
        var6 = 'INCONNU'	# mettre inconnu
    requete_insertion = "insert into client_output(id , first_name , last_name ,	email ,	gender , ville)" \
        "values (%s,'%s','%s','%s','%s','%s')" % ( var1, var2, var3, var4, var5[0], var6)
    curseur_dest.execute(requete_insertion)
###################################### chargement  et transformation de fichier mysql
db_source=mysql.connect(host="localhost",user="root",passwd="",db="input")
curseur_source = db_source.cursor()
requete_selection="select * from client_data"
curseur_source.execute(requete_selection)
for tmp in curseur_source:
    requete_insertion="insert into client_output (id , first_name , last_name ,	email ,	gender , ville)" \
                      "values (%s,'%s','%s','%s','%s','%s')" % (tmp[0],tmp[1],tmp[2],tmp[3],tmp[4],tmp[5])

    curseur_dest.execute(requete_insertion)
db_destination.commit()
db_destination.close()
db_source.close()