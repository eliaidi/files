import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import static groovyx.gpars.GParsPool.withPool

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log
csv = new File('/tmp/jessi.csv'); csv.write('')
log = { text -> out << text + "\n" }
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit) {
    def body = '''{
                    "query":{"bool":{"must":[{"range":{"shipment.shipping_option.estimated_delivery.date":
                    {"lt":"2014-12-22T00:00:00.000-03:00"}}},{"query_string":{"default_field":"shipment.status",
                    "query":"ready_to_ship shipped"}},{"range":{"shipment.date_created":{"gte":"2014-11-01T00:00:00.000-03:00"}}}],
                    "must_not":[{"term":{"shipment.service_id":"81"}}],
                    "should":[]}},
					"from": '''+offset+''',
					"size": '''+limit+''',
					"sort": [],
					"facets": {}
                }'''
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Métodos *******************************************

def getShipments = { 
	int offset = 0
	int LIMIT = 5000

	def data = getData(offset, LIMIT)
	log "Getting ${data.total} shipments"

	def shipments = data.shipments
	offset += LIMIT
	addOrders(shipments)
	
	while (offset < data.total) {
	  shipments = getData(offset, LIMIT).shipments
	  log "Processed ${offset} of ${data.total}"
	  addOrders(shipments)

	  offset += LIMIT
	}
}

def addOrders(def ships) {
	log "adding orders..."
	withPool(7) {
		ships.eachParallel{
			addOrder(String.valueOf(it.order_id))
		}
	}
	log "end adding"
}

synchronized void addOrder(String orderId) {
	csv << orderId + "\n"
}

// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	getShipments()

} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------