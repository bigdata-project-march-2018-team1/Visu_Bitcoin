import bottle
import datetime
 
width = str(1500)
height = str(800)

# Default route
@bottle.route("/")
def index():
	bottle.redirect("/lastdays/30")

# Show last hour
@bottle.route("/lasthour")
@bottle.view("page.tpl")
def index() :
	datestart="now-1h"
	iframe="<iframe src=\"http://localhost:5601/app/kibana#/dashboard/cb2e3d00-2c49-11e8-b68c-239c34f5932e?embed=true&_g=(refreshInterval%3A('%24%24hashKey'%3A'object%3A202'%2Cdisplay%3A'5%20seconds'%2Cpause%3A!f%2Csection%3A1%2Cvalue%3A5000)%2Ctime%3A(from%3A"+datestart+"%2Cmode%3Aquick%2Cto%3Anow))\" height="+height+" width="+width+"></iframe>"
	return {"datestart":datestart, "dateend":"now", "iframe":iframe }
	
# Show x last days
@bottle.route("/lastdays/<days:int>")
@bottle.view("page.tpl")
def index(days) :
	datestart="now-"+str(days)+"d"
	iframe="<iframe src=\"http://localhost:5601/app/kibana#/dashboard/cb2e3d00-2c49-11e8-b68c-239c34f5932e?embed=true&_g=(refreshInterval%3A('%24%24hashKey'%3A'object%3A202'%2Cdisplay%3A'5%20seconds'%2Cpause%3A!f%2Csection%3A1%2Cvalue%3A5000)%2Ctime%3A(from%3A"+datestart+"%2Cmode%3Aquick%2Cto%3Anow))\" height=\"700\" width=\"1500\"></iframe>"
	return {"datestart":datestart, "dateend":"now", "iframe":iframe }

 
# Show from date to date
@bottle.post('/fromdatetodate')
@bottle.view("page.tpl")
def index():
	datestart = bottle.request.forms.get('datestart')
	dateend = bottle.request.forms.get('dateend')
	iframe="<iframe src=\"http://localhost:5601/app/kibana#/dashboard/cb2e3d00-2c49-11e8-b68c-239c34f5932e?embed=true&_g=(refreshInterval%3A('%24%24hashKey'%3A'object%3A202'%2Cdisplay%3A'5%20seconds'%2Cpause%3A!f%2Csection%3A1%2Cvalue%3A5000)%2Ctime%3A(from%3A'"+datestart+"'%2Cmode%3Aabsolute%2Cto%3A'2018-03-28T21%3A59%3A59.999Z'))\" height=\"700\" width=\"1500\"></iframe>"
	return {"datestart":datestart, "dateend":dateend, "iframe":iframe }

@bottle.route('/static/<filename:path>')
def server_static(filename):
	print (filename)
	return bottle.static_file(filename, root='static')
	
# 404 Error
@bottle.error(404)
def error404(error):
	return '<h2>Page inexistante.</h2><br /><a href="/">Retourner au tableau de bord</a>'

# Run server
def main():
	bottle.run(bottle.app(), host='0.0.0.0', port=8080, debug= True, reloader=True)

if __name__ == '__main__':
	main()