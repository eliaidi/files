import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import java.math.*
import mercadoenvios.constants.ShipmentSubstatus
import static groovyx.gpars.GParsPool.withPool

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/outAll.log'); out.write('') //Archivo de log

synchronized void log (text) { out << text + "\n" }
trackingStatusService = ctx.getBean('trackingStatusService')

getTime = { init , end -> def time; use(groovy.time.TimeCategory) { time = end - init } }
getHours = { init, end -> def time; time = getTime(init, end); time.days*24 + time.hours }

// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit) {
    def body = '''{
                    "query":{"bool":{"must":[{"term":{"shipment.status":"delivered"}},{"query_string":{"default_field":"shipment.substatus","query":"fulfilled_feedback no_action_taken"}},{"match_all":{}}],"must_not":[],"should":[]}},
					"from": '''+offset+''',
					"size": '''+limit+''',
					"sort": [],
					"facets": {}
                }'''
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Métodos *******************************************

/** Obtiene los shipments de elastic, procesando pagina por pagina. Devuelve lista completa de shipments */
def getShipments = {
	int offset = 0
	int LIMIT = 10000

	def data = getData(offset, LIMIT)
	log "Getting ${data.total} shipments from service"

	def shipments = data.shipments
	offset += LIMIT
	
	while (offset < data.total) {
	  shipments.addAll(getData(offset, LIMIT).shipments)
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
	/*
	shipments.groupBy{it.service_id}.each { serviceId, ships ->
		log "service: $serviceId"
		calculate(ships)
	}
	shipments.groupBy{it.site_id}.each { siteId, ships ->
		log "siteId: $siteId"
		calculate(ships)
	}
	*/
	calculate(shipments)
}

def getNotDeliveredEvent(trackingNumber, serviceId) {
	def event = TrackingEvents.findAllByTrackingNumber(trackingNumber).find{ 
		trackingStatusService.getTrackingStatus(it.trackingStatus, serviceId).normalizedStatus == "not_delivered"
	}
	return event
}

/** Calcula tiempos de diferencia entre shipping y fecha de entrega (en días hábiles)*/
def calculate(def ships) {
	log "calculating..."
	log "total: ${ships.size()}"
	def acumDiff = new java.util.concurrent.atomic.AtomicInteger()
	def totalWrong = new java.util.concurrent.atomic.AtomicInteger()
	def addMap = Collections.synchronizedMap(new HashMap<Integer,Integer>())
	addMap.put(24, 0); addMap.put(48, 0); addMap.put(72, 0); addMap.put(96, 0); 
	addMap.put(120, 0); addMap.put(144, 0); addMap.put(168, 0); addMap.put(192, 0); 
	addMap.put(9999, 0)
	withPool() {
		ships.eachParallel { s ->
			def notDeliEvent = getNotDeliveredEvent(s.tracking_number, s.service_id)
			if (notDeliEvent) {
				def diff = getHours(parseDate(s.status_history.date_delivered), notDeliEvent.dateCreated)
				acumDiff.getAndAdd(diff)
				totalWrong.getAndIncrement()
				log "${s.tracking_number} - ${diff} - delivered: ${s.status_history.date_delivered} - not_delivered: ${s.status_history.date_not_delivered}"

				def k = addMap.findAll{it.key >= diff}.keySet().sort{it}.first()
				addMap[(k)]++
			}	
		}
	}
	def avg = totalWrong ? acumDiff / totalWrong : 0
	log "hs acum diff: $acumDiff - wrong: $totalWrong - avg: $avg"
	log "simulation: ${addMap}"
}


// ---------------------------------------------------------------------------------------------------------------

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}



// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	
	getShipments()
	
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------