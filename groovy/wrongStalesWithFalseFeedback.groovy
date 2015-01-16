import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import java.math.*
import mercadoenvios.constants.ShipmentSubstatus
import static groovyx.gpars.GParsPool.withPool
import java.util.concurrent.atomic.AtomicInteger
import mercadoenvios.utils.JsonUtil

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log
ok = new File('/tmp/ok.csv'); ok.write('')
falseFb = new File('/tmp/falseFb.csv'); falseFb.write('')
hasMed = new File('/tmp/hasMed.csv'); hasMed.write('')
hasntMed = new File('/tmp/hasntMed.csv'); hasntMed.write('')
noOrd = new File('/tmp/noOrd.csv'); noOrd.write('')
synchronized void log(text) { out << text + "\n" }
synchronized void isOk(text) { ok << text + "\n" }
synchronized void falseFeedback(text) { falseFb << text + "\n" }
synchronized void hasMediation(text) { hasMed << text + "\n" }
synchronized void hasntMediation(text) { hasntMed << text + "\n" }
synchronized void noOrder(text) { noOrd << text + "\n" }
//falseFeedback.size == hasMediation.size + hasntMediation.size
//ok.size + falseFeedback.size == total size query elastic

ordersService = ctx.getBean('ordersService')
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit) {
    def body = '''{
                    "query":{
                    	"bool":{
                    		"must":[{"query_string":{"default_field":"shipment.status","query":"delivered"}},
                    			{"query_string":{"default_field":"shipment.substatus","query":"no_action_taken"}},
                    			{"range":{"shipment.status_history.date_delivered":{"gte":"2014-11-01T00:00:00.000Z"}}}],
                    		"must_not":[],
                    		"should":[]}},
					"from": '''+offset+''',
					"size": '''+limit+''',
					"sort": [],
					"facets": {}
                }'''
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Métodos *******************************************

def processShipments = {
	int offset = 0
	int LIMIT = 1000

	def data = getData(offset, LIMIT)
	log "Getting ${data.total} shipments"

	def shipments = data.shipments
	analyse(shipments)
	offset += LIMIT
	
	while (offset < data.total) {
	  sleep(5000)
	  shipments = getData(offset, LIMIT).shipments
	  analyse(shipments)
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
}

def isNegativeFeedbackFirst(def order, def shipment) {
	return order.feedback.purchase?.fulfilled == false &&
			parseDate(order.feedback.purchase.date_created) < parseDate(shipment.status_history.date_delivered)
}

def analyse(def ships) {
	withPool() {
		ships.eachParallel { s ->
			try {
				def order = JsonUtil.cleanJsonNulls(ordersService.getOrdersData(String.valueOf(s.order_id), Long.valueOf(s.sender_id)))
				if (order) {
					String dataOut = "${order.id},${s.id},${s.tracking_number}"
					if (isNegativeFeedbackFirst(order, s)) {
						falseFeedback(dataOut)
						if (order.mediations) {
							hasMediation(dataOut)
						} else {
							hasntMediation(dataOut)
						}
					} else {
						isOk(dataOut)
					}
				} else {
					noOrder("${s.id}")
				}
			} catch (Exception e) {
				log "Exception: ${e.getMessage()} - ${e.getStackTrace()}"
			}
		}
	}
}

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	
	processShipments()
	
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------