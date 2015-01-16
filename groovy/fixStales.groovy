import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import java.math.*
import mercadoenvios.constants.ShipmentSubstatus
import static groovyx.gpars.GParsPool.withPool
import grails.util.GrailsUtil
import grails.converters.JSON


// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log

synchronized void log (text) { out << text + "\n" }
mongoService = ctx.getBean('mongoService')
restClient = ctx.getBean('restClient')
trackingStatusService = ctx.getBean('trackingStatusService')
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit) {
    def body = '''{
                    "query":{"bool":{"must":[{"term":{"shipment.status":"delivered"}},
                    {"query_string":{"default_field":"shipment.substatus","query":"fulfilled_feedback no_action_taken"}}],
                    "must_not":[{"constant_score":{"filter":{"missing":{"field":"shipment.status_history.date_not_delivered"}}}}],"should":[]}},
					"from": '''+offset+''',
					"size": '''+limit+''',
					"sort": [],
					"facets": {}
                }'''
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Métodos *******************************************

/** Obtiene los shipments de elastic, procesando pagina por pagina. Devuelve lista completa de shipments */
def processShipments = {
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
	proc(shipments)
}

/** Calcula tiempos de diferencia entre shipping y fecha de entrega (en días hábiles)*/
def proc(def ships) {
	log "calculating..."
	log "total: ${ships.size()}"
	def results = [changed: [], not_changed: [], not_found: []]
	ships.each { s ->
		def mongoMessage = [shipping_id: s.id, type: "not_delivered"]
		def msg = mongoService.find("mails", mongoMessage)
		if (msg) {
			mongoService.remove("mails", mongoMessage)
		} else {
			results.not_found << s.id
		}
		def r = putNotDelivered(s.id, s.tracking_number, Long.valueOf(s.service_id))
		if (r) {
			results.changed << s.id
		} else {
			results.not_changed << s.id
		}
	}
	log "sizes: ${results.collect{[(it.key): it.value.size()]}}"
	log "results: $results"
}

def putNotDelivered(shipmentId, trackingNumber, serviceId) {

	def result
	String uri = "/shipments/${shipmentId}?caller.scopes=admin"
	
	def substatus = getSubstatus(trackingNumber, serviceId)
	if (substatus != "No_event") {
	
		def body = substatus ? [status: "not_delivered", substatus: substatus] : [status: "not_delivered"]
		log "PUT a ${shipmentId}: ${body}"
		
		def jsonData = (body as JSON).toString()

		restClient.put(uri: uri,
			data: jsonData,
			success:{
				result = it.data
			},
			failure: {
				if(it.exception) {
					log "Error llamando a ${uri} - StackTrace: ${GrailsUtil.sanitize(it.exception)}"
				} else {
					int statusCode = it?.status?.getStatusCode()
					log "Server returned error code: ${it.status} ${uri}"
				}
			}
		)
	}
	return result
}	

def getSubstatus(String trackingNumber, Long serviceId) {
	def events = TrackingEvents.findAllByTrackingNumber(trackingNumber).findAll{ 
		trackingStatusService.getTrackingStatus(it.trackingStatus, serviceId).normalizedStatus == "not_delivered"
	}
	def sortedEvents = events.sort{it.eventDate}
	def lastEvent = sortedEvents ? sortedEvents.last() : "No_event"

	return lastEvent != "No_event" ? trackingStatusService.getTrackingStatus(lastEvent.trackingStatus, serviceId).substatus : "No_event"
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