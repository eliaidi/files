import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import java.math.*
import mercadoenvios.constants.ShipmentSubstatus
import static groovyx.gpars.GParsPool.withPool
import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH
import mercadoenvios.utils.JsonUtil

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log
mla = new File('/tmp/stale4-MLA.csv'); mla.write('')
mlb = new File('/tmp/stale4-MLB.csv'); mlb.write('')
mlm = new File('/tmp/stale4-MLM.csv'); mlm.write('')
noOrder = new File('/tmp/noOrder.csv'); noOrder.write('')
otherStatus = new File('/tmp/otherStatus.csv'); otherStatus.write('')
synchronized void log(text) { out << text + "\n" }
synchronized void addStale4MLA(text) { mla << text + "\n" }
synchronized void addStale4MLB(text) { mlb << text + "\n" }
synchronized void addStale4MLM(text) { mlm << text + "\n" }
synchronized void addNoOrder(text) { noOrder << text + "\n" }
synchronized void addOtherStatus(text) { otherStatus << text + "\n" }

computeHandlingTimeService = ctx.getBean('computeHandlingTimeService')
siteEstimatedDeliveryService = ctx.getBean('siteEstimatedDeliveryService')
ordersService = ctx.getBean('ordersService')

def addStale4(text, siteId) {
	switch (siteId) {
		case "MLA": addStale4MLA(text)
			break
		case "MLB": addStale4MLB(text)
			break
		case "MLM": addStale4MLM(text)
			break
		default: log "Wrong siteId: $siteId"
			break
	}
}

// ************************************************ Elastic *******************************************
getData = { offset, limit, serviceId ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit, serviceId), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}
def getQuery(offset, limit, serviceId) {
	def body = '''{
                    "query":{
                    	"bool":{
                    		"must":[{
                    			"range":{"shipment.status_history.date_shipped":{
                    				"from":"2014-12-01T00:00:00.000-03:00",
                    				"to":"2014-12-06T00:00:00.000-03:00"}}},
                    			{"term":{"shipment.mode":"me2"}},
                    			{"term":{"shipment.service_id":"'''+serviceId+'''"}}],
                    		"must_not":[],
                    		"should":[]
                    	}
                    },
					"from": '''+offset+''',
					"size": '''+limit+''',
					"sort": [],
					"facets": {}
                }'''
}

// ************************************************ Métodos *******************************************
/** Obtiene los shipments de elastic, procesando pagina por pagina. Devuelve lista completa de shipments */
def processShipments = { serviceId ->
	int offset = 0
	int LIMIT = 5000

	log "Getting shipments from service $serviceId"
	def data = getData(offset, LIMIT, serviceId)
	log "Found ${data.total} shipments"

	doProcess(data.shipments)
	log "First ${LIMIT} processed"
	offset += LIMIT
	
	while (offset < data.total) {
	  sleep(1000)
	  doProcess(getData(offset, LIMIT, serviceId).shipments)
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
}

def getMediations(orderId, callerId) {
    String uri = "/mediations/claims/search?order_id=${orderId}&caller.id=${callerId}".toString()

    def dataResponse = null
    ctx.getBean('restClient').get(uri: uri,
        success: {
            dataResponse = it.data
        },
        failure: {
            if(it.exception)
                println "Calling to ${uri}"
            else
                println "Server returned error code: ${it?.status?.statusCode} ${uri} when getClaimForOrderId for orderId = ${orderId}"
        })

    return dataResponse
}

boolean hadNoMediationAtThatTime(order, shipment, dateStaleDelivered) {
	def mediations = getMediations(order, shipment.sender_id)
	boolean noMediation
	if (mediations?.total > 1) {
		noMediation = parseDate(mediations.data.sort{it.date_created}.first().date_created) > dateStaleDelivered
	} else if (mediations?.total == 1) {
		noMediation = parseDate(mediations.data.first().date_created) > dateStaleDelivered
	} else {
		noMediation = true
	}
	return noMediation
}

boolean wasFalseFeedback(order, dateStaleDelivered) {
	return order.feedback.purchase?.fulfilled == false && 
		parseDate(order.feedback.purchase?.date_created) < dateStaleDelivered
}

boolean wasFalseFeedbackAndHadNoMediationAtThatTime(order, shipment, dateStaleDelivered) {
	 wasFalseFeedback(order, dateStaleDelivered) && hadNoMediationAtThatTime(order, shipment, dateStaleDelivered)
}

boolean isStale4(shipment, dateStaleDelivered) {
	def order
	try {
		order = JsonUtil.cleanJsonNulls(ordersService.getOrdersData(String.valueOf(shipment.order_id), Long.valueOf(shipment.sender_id)))
		if (order) {
			return wasFalseFeedbackAndHadNoMediationAtThatTime(order, shipment, dateStaleDelivered)
		} else {
			addNoOrder("${shipment.id}")
		}
	} catch(Exception e) {
		addNoOrder("${shipment.id}")
	}
}

def doProcess(def ships) {
	log "calculating..."
	withPool() {
		ships.eachParallel { s ->
			def ship = JsonUtil.cleanJsonNulls(s)
			def maxStaleDays = CH.config.stale.shipped.services[Long.valueOf(ship.service_id)]
			if (maxStaleDays) {
				maxStaleDays += 5
				def dateStale = parseDate(ship.shipping_option.estimated_delivery.date) + maxStaleDays
				if (ship.status_history.date_delivered) {
					processIfIsStale(ship, parseDate(ship.status_history.date_delivered), dateStale)
				} else if (ship.status_history.date_not_delivered) {
					processIfIsStale(ship, parseDate(ship.status_history.date_not_delivered), dateStale)
				} else if (ship.status == 'shipped') {
					processIfIsStale4(ship, dateStale)
				} else {
					addOtherStatus("${ship.id}")
				}
			}
		}
	}
}

def processIfIsStale4(shipment, dateStale) {
	if (isStale4(shipment, dateStale)) {
		addStale4("${shipment.order_id}, ${shipment.id}, ${shipment.tracking_number}", shipment.site_id)
	}
}

def processIfIsStale(shipment, dateFinalStatus, dateStale) {
	if (dateFinalStatus > dateStale) {
		//Es stale
		processIfIsStale4(shipment, dateStale)		
	}
}

//Funciones útiles para tratar las fechas
String formatDate(Date date) {
  ISODateTimeFormat.dateTime().print(new DateTime(date.getTime()))
}
Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}
Calendar parseCalendar(String date) {
	def cal = Calendar.instance
	cal.setTime(parseDate(date))
	cal
}
Calendar toCalendar(Date date) {
	def cal = Calendar.instance
	cal.setTime(date)
	cal
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {

	[21L,22L,23L,61L,62L,63L,64L,151L,152L,81L,171L,181L].each { processShipments(it) }

} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------