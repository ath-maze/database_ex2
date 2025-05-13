import sys, os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
from bottle import route, run, static_file, request
import pymysql as db
import app

def renderTable(tuples):
    # this function accepts a list that contains tuples
    # it then iterates through the list of tuples and creates an html table that displays the results
    # the first tuple in the list is going to be the table header, <th> element, and the rest of the values are written row-by-row
    # more about html tables here: https://www.w3schools.com/html/html_tables.asp
    # for the python join() function read here: https://www.tutorialspoint.com/python/string_join.htm	

    printResult = """<style type='text/css'> h1 {color:red;} h2 {color:blue;} p {color:green;} </style>
    <table border = '1' frame = 'above'>"""
    header='<tr><th>'+'</th><th>'.join([str(x) for x in tuples[0]])+'</th></tr>'
    data='<tr>'+'</tr><tr>'.join(['<td>'+'</td><td>'.join([str(y) for y in row])+'</td>' for row in tuples[1:]])+'</tr>'
        
    printResult += header+data+"</table>"
    return printResult

@route('/checkIfPropertyExists')
def checkIfPropertyExistsWEB():
    location_a = request.query.location_a
    property_type_a = request.query.property_type_a
    table = app.checkIfPropertyExists(location_a, property_type_a)
    return "<html><body>" + renderTable(table) + "</body></html>"
    
@route('/selectTopNhosts')
def selectTopNhostsWEB():
    n = request.query.n
    table = app.selectTopNhosts(n)
    return "<html><body>" + renderTable(table) + "</body></html>"

@route('/findMatchingProperties')
def findMatchingPropertiesWEB():
    guest_id = request.query.guestId or "Unknown guest_id"     
    table = app.findMatchingProperties(guest_id)
    return "<html><body>" + renderTable(table) + "</body></html>"
	
@route('/countWordsForProperties')
def countWordsForPropertiesWEB():
    n = request.query.n
    m = request.query.m
    table = app.countWordsForProperties(n,m)
    return "<html><body>" + renderTable(table) + "</body></html>"

@route('/findCommonPropertiesAndGuests')
def findCommonPropertiesAndGuestsWEB():
    a = request.query.guestIdA
    b = request.query.guestIdB
    table = app.findCommonPropertiesAndGuests(a,b)
    return "<html><body>" + renderTable(table) + "</body></html>"

@route('/highValueHost')
def highValueHostWEB():
    min_price_booking = request.query.min_price
    min_rating_review = request.query.min_rating
    min_avg_price_host = request.query.min_avg_price
    min_avg_rating_host = request.query.min_avg_rating     
    table = app.highValueHost(min_price_booking, min_rating_review, min_avg_price_host, min_avg_rating_host)
    return "<html><body>" + renderTable(table) + "</body></html>"

@route('/recommendProperty')
def recommendPropertyWEB():
    guest_id = request.query.guestId
    desired_city = request.query.city
    desired_amenities = request.query.amenities
    max_price = request.query.max_price
    min_rating = request.query.min_rating
    table = app.recommendProperty(guest_id, desired_city, desired_amenities, max_price, min_rating)
    return "<html><body>" + renderTable(table) + "</body></html>"
    
@route('/:path')
def callback(path):
    return static_file(path, 'web')

@route('/')
def callback():
    return static_file("index.html", 'web')

run(host='localhost', port=8080, reloader=True, debug=True)
