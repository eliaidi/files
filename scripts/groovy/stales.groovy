import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import java.math.*
import mercadoenvios.constants.ShipmentSubstatus
import static groovyx.gpars.GParsPool.withPool
import java.util.concurrent.atomic.AtomicInteger

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log

log = { text -> out << text + "\n" }
ordersService = ctx.getBean('ordersService')
paid = new AtomicInteger()
notPaid = new AtomicInteger()
notLocalized = new AtomicInteger()
fulfilled = new AtomicInteger()
noAction = new AtomicInteger()
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/logstash/_search', getQuery(offset, limit), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [logs: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit) {
    def body = '''{
                    "query":{
                    	"bool":{
                    		"must":[{"term":{"log.message":"should"}}],
				            "must_not": [],
				            "should": []
				        }
				    },
					"from": '''+offset+''',
					"size": '''+limit+''',
					"sort": [],
					"facets": {}
                }'''
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Métodos *******************************************

def getLogs = {
	int offset = 0
	int LIMIT = 5000

	def data = getData(offset, LIMIT)
	log "Getting ${data.total} shipments"

	def logs = data.logs
	offset += LIMIT
	
	while (offset < data.total) {
	  logs.addAll(getData(offset, LIMIT).logs)
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
	logs
}

def analyse(def logs) {
	log "calculating..."
	def still = new AtomicInteger(logs.size())
	def orderedLogs = logs.sort{parseDate(it['@timestamp'])}
	log "first: ${orderedLogs.first()}"
	log "last: ${orderedLogs.last()}"
	withPool() {
		logs.eachParallel { l ->
			try {
				def m = l.message.split(" ")
				def shipId
				def status
				def substatus
				if (m.size() > 10) {
					shipId = m[1] as long
					status = m[7]
					substatus = m[10]
				} else if (m.size() > 7) {
					shipId = m[1] as long
					status = m[7]
				}
				def ship = Shipment.get(shipId)
				def order = ordersService.getOrdersData(ship.orderId, ship.senderId)
				if (order) {
					if (order.status == 'paid') {
						paid.getAndIncrement()
						if (status == 'delivered') {
							if (substatus == 'fulfilled_feedback')
								fulfilled.getAndIncrement()
							if (substatus == 'no_action_taken')
								noAction.getAndIncrement()
						}
						if (status == 'not_delivered')
							notLocalized.getAndIncrement()
					} else {
						notPaid.getAndIncrement()
					}
				}
			} catch (Exception e) {
				log "Exception: ${e.getMessage()} - ${e.getStackTrace()}"
			}
			def toFinish = still.getAndDecrement()
			if (toFinish % 500 == 0)
				log "toFinish: $toFinish"
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
	
	analyse(getLogs())
	log "paid: $paid"
	log "noAction: $noAction"
	log "fulfilled: $fulfilled"
	log "notLocalized: $notLocalized"
	log "notPaid: $notPaid"
	
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------