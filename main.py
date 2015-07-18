#NAME: NIRZARI IYER
#Assignment-3
#ID NUMBER: 1001117633
#BATCH TIME- 6:00 to 8:00 p.m.
import MySQLdb
import io
import os
import cloudstorage as gcs
import csv
import timeit
import json
from bottle import Bottle
from google.appengine.api import app_identity
from StringIO import StringIO
from bottle import route, request, response, template, get, HTTPResponse

bottle = Bottle()

#location of file into default bucket on google cloud storage
bucket_name = os.environ.get('BUCKET_NAME', app_identity.get_default_gcs_bucket_name())
bucket = '/' + bucket_name
filename = bucket + '/earthquake.csv'

#declare cursor globally
connobj = MySQLdb.connect(unix_socket='/cloudsql/cloudcomp2-979:simple' ,user='root')
c = connobj.cursor()

#Get filename from user
@bottle.route('/uploadform')
def uploadform():
    return template('upload_form')

#Upload file into bucket on google cloud storage
@bottle.route('/uploadfile',  method='POST')
def uploadfile():
    #Calculate start time
    start = timeit.default_timer()
    filecontent = request.files.get('filecontent')
    rawfilecontent = filecontent.file.read()
    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open(filename,'w',content_type='text/plain',retry_params=write_retry_params)
    gcs_file.write(rawfilecontent)
    gcs_file.close()
    #Calculate end time
    stop = timeit.default_timer()
    #Calculate total time
    time_taken = stop - start
    return template('upload_file',time_taken=time_taken)

#Read data from bucket and Insert data into google MySQLdb
def parse(filename, delimiter,c):
    with gcs.open(filename, 'r') as gcs_file:
        csv_reader = csv.reader(StringIO(gcs_file.read()), delimiter=',',
                     quotechar='"')
	# Skip the header line
        csv_reader.next()   
        try:
	    start = timeit.default_timer()
            for row in csv_reader:
	        time = timestamp(row[0])
		updated = timestamp(row[12])
                for i in range (0,14):
		    if row[i] == '':
                        row[i] = "''"
		place = str(row[13])
		place = place.replace("'","")
                insert = "INSERT INTO earthquake (time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated,\
                         place, type) values('"+time+"',"+row[1]+","+row[2]+","+row[3]+","+row[4]+",'"+row[5]+"',"+row[6]+","+row[7]+",\
                         "+row[8]+","+row[9]+",'"+row[10]+"','"+row[11]+"','"+updated+"','"+place+"','"+row[14]+"')"
                c.execute(insert) 
	    stop = timeit.default_timer()
            insert_time = stop - start	    
            return insert_time

        except Exception as e:
            print ("Data can't be inserted" + str(e))

#coverting time format 
def timestamp(string):
    ans = string[:10] + ' ' + string[11:19]
    return ans

#query to get result for different magnitude for each week
def query(mag,c):
    query = 'SELECT week(time) as week, count(*) as count, mag as mag FROM earthquake WHERE mag = '+str(mag)+' GROUP BY week(time), mag'
    c.execute(query)
    ans_query = c.fetchall()
    return ans_query

#query for magnitude greater than 5
def bigquery(mag,c):
    query = 'SELECT week(time) as week, count(*) as count, mag as mag FROM earthquake WHERE mag > '+str(mag)+' GROUP BY week(time), mag'
    c.execute(query)
    ans_query = c.fetchall()
    return ans_query

#function to format generated result
def ans_format(mag):
    table = "<table border='2'><tr><th>Week</th><th>Number of quakes</th><th>Magnitude</th></tr>"
    ans = ""
    for x in mag:
        ans = ans +"<tr><td>" + str(x[0]) + "</td><td>" + str(x[1]) + "</td><td>" + str(x[2]) +"</td></tr>"
    table += ans + "</table>"
    return table	
  
#Displays the webinterface for user to enter magnitude and location
@bottle.route('/webinterface')
def webinterface():
    return template('webinterface')


@bottle.route('/dynamic_query', method = "POST")
def dynamic_query():
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        dict_data = request.forms.dict
        print dict_data
        query_final = create_query(dict_data)
        connectdb = 'USE db'
        c.execute(connectdb)
	query_ans = c.execute(query_final)
        query_result = c.fetchall()
	print query_result
	query_output = query_format(query_result)
	print query_output        
	return HTTPResponse(body=str(query_output), status=200)

#function to create dynamic query
def create_query(dict_data):
    q1 = "SELECT * FROM earthquake WHERE "
    q2 = "mag "
    param1 = ""
    if dict_data["param1"][0] == "eq":
	    param1 = "= "
    elif dict_data["param1"][0]== "gt":
	    param1 = "> "
    elif dict_data["param1"][0] == "gte":
	    param1 = ">= "
    elif dict_data["param1"][0] == "lt":
	    param1 = "< "
    elif dict_data["param1"][0] == "lte":
	    param1 = "<= "
    q3 = param1
    mag = dict_data["mag"][0]
    q4 = mag
    param2 = ""
    if dict_data["param2"][0] == "or":
        param2 = " or "
    elif dict_data["param2"][0] == "and":
        param2 = " and "
    q5 = param2
    q6 = "place LIKE "
    loc = dict_data["loc"][0]
    q7 = loc
    query_final = str(q1 + q2 + q3 + q4 + q5 + q6 + "'%" +q7+ "%'")
    return query_final


def query_format(query_result):
    table = "<table border='2'><tr><th>time</th><th>latitude</th><th>longitude</th><th>depth</th><th>mag</th><th>magType</th><th>nst</th>"\
        "<th>gap</th><th>dmin</th><th>rms</th><th>net</th><th>id</th><th>updated</th><th>place</th><th>type</th></tr>"
    ans = ""
    for x in query_result:
        print x
        ans += "<tr>"
        ans += "<td>"+x[0].strftime("%d/%m/%Y %H:%M:%S")+"</td>"
        ans += "<td>"+str(x[1])+"</td>"
        ans += "<td>"+str(x[2])+"</td>"
        ans += "<td>"+str(x[3])+"</td>"
        ans += "<td>"+str(x[4])+"</td>"
        ans += "<td>"+str(x[5])+"</td>"
        ans += "<td>"+str(x[6])+"</td>"
        ans += "<td>"+str(x[7])+"</td>"
        ans += "<td>"+str(x[8])+"</td>"
        ans += "<td>"+str(x[9])+"</td>"
        ans += "<td>"+str(x[10])+"</td>"
        ans += "<td>"+str(x[11])+"</td>"
        ans += "<td>"+x[12].strftime("%d/%m/%Y %H:%M:%S")+"</td>"
        ans += "<td>"+str(x[13])+"</td>"
        ans += "<td>"+str(x[14])+"</td>"
        ans += "</tr>"
    table += ans + "</table>"
    return table

@bottle.route('/')
def main():
    try:
        createdb = 'CREATE DATABASE IF NOT EXISTS db'
        c.execute(createdb)
        connectdb = 'USE db'
        c.execute(connectdb)
        table = 'CREATE TABLE IF NOT EXISTS earthquake '\
                '(time TIMESTAMP,'\
                'latitude DOUBLE,'\
                'longitude DOUBLE,'\
                'depth DOUBLE,'\
                'mag DOUBLE,'\
                'magType varchar(500),'\
                'nst DOUBLE,'\
                'gap DOUBLE,'\
                'dmin DOUBLE,'\
                'rms DOUBLE,'\
                'net varchar(500),'\
                'id varchar(500),'\
                'updated TIMESTAMP,'\
                'place VARCHAR(500),'\
                'type VARCHAR(500))'
        c.execute(table)
        insert_time = parse(filename,',',c)
        mag2 = query(2,c) 
	mag3 = query(3,c)
	mag4 = query(4,c)
	mag5 = query(5,c)
	maggt5 = bigquery(5,c)
	ans_mag2 = ans_format(mag2)
	ans_mag3 = ans_format(mag3)
	ans_mag4 = ans_format(mag4)
	ans_mag5 = ans_format(mag5)
	ans_maggt5 = ans_format(maggt5)  
        ans = "Final Result: <br><br> Time taken to Insert data into MySQL database is: <br>" +str(insert_time)+"<br><br>" \
	    "Earthquake of magnitude 2: <br> "+str(ans_mag2)+"<br><br> Earthquake of magnitude 3: <br>" \
            +str(ans_mag3)+ "<br><br> Earthquake of magnitude 4: <br>" +str(ans_mag4)+ "<br><br> Earthquake" \
	    "of magnitude 5: <br>" +str(ans_mag5)+ "<br><br> Earthquake of magnitude greater than 5: <br>" +str(ans_maggt5)	
	return ans

    except Exception as e:
        print str(e)
        return e

# Define an handler for 404 errors.
@bottle.error(404)
def error_404(error):
    """Return a custom error 404."""
    return 'Sorry, nothing at this URL.'
# [END all]
