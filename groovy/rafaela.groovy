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
out = new File('/tmp/out.log'); out.write('') //Archivo de log

log = { text -> out << text + "\n" }
rulesMongoMLAService = ctx.getBean('rulesMongoMLAService')
rulesMongoMLAAgenciesService = ctx.getBean('rulesMongoMLAAgenciesService')
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService') //Para calcular los días hábiles

rulesByServices = [
	61L: [ruleService: rulesMongoMLAService, field: "speed_from_ws"],
	62L: [ruleService: rulesMongoMLAService, field: "speed_from_ws"],
	63L: [ruleService: rulesMongoMLAService, field: "speed_from_ws"],
	64L: [ruleService: rulesMongoMLAService, field: "speed_from_ws"],
	151L: [ruleService: rulesMongoMLAAgenciesService, field: "speed"],
	152L: [ruleService: rulesMongoMLAAgenciesService, field: "speed"]
]
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit, serviceIds ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit, serviceIds), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit, serviceIds) {
	def servIds = 61L in serviceIds ? "61 62 63 64" : "151 152"
	log "quering: $servIds"
    def body = '''{
                    "query":{"bool":{"must":[{"term":{"shipment.receiver_address.zip_code":"2300"}},
                    {"query_string":{"default_field":"shipment.service_id","query":"'''+servIds+'''"}},
                    {"range":{"shipment.date_created":{"gte":"2014-12-01T00:00:00.000Z"}}},
                    {"term":{"shipment.status":"delivered"}}],
                    "must_not":[{"constant_score":{"filter":{"missing":{"field":"shipment.status_history.date_delivered"}}}},
                    {"constant_score":{"filter":{"missing":{"field":"shipment.status_history.date_shipped"}}}}],"should":[]}
				    },
					"from": '''+offset+''',
					"size": '''+limit+''',
					"sort": [],
					"facets": {}
                }'''
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Métodos *******************************************

/** Obtiene los shipments de elastic, procesando pagina por pagina. Devuelve lista completa de shipments */
def processShipments = { serviceIds ->
	int offset = 0
	int LIMIT = 1000

	def data = getData(offset, LIMIT, serviceIds)
	log "Getting ${data.total} shipments from service $serviceIds"

	calculate(data.shipments)
}

def calculate(def ships) {
	log "calculating..."
	log "size: ${ships.size()}"
	def shipmentsAndTimes = ships.collect {
		sleep(100)
		def params = rulesByServices[it.service_id as long]
		def rule = params.ruleService.getRule(it.shipping_option.id as long)
		if (rule) {
			int speed = rule[(params.field)]
			[
				speed: speed,
				took: computeHandlingTimeService.getWorkingDays(
					parseDate(it.status_history.date_shipped),
					parseDate(it.status_history.date_delivered),
					"MLA"
				)
			]
		}
	}

	log "shipmentsAndTimes: ${shipmentsAndTimes.size()}"
	shipmentsAndTimes = shipmentsAndTimes.findAll{it}
	log "shipmentsAndTimes: ${shipmentsAndTimes.size()}"
		
	log "shipmentsAndTimes: $shipmentsAndTimes"
}

//Funciones útiles para tratar las fechas
String formatDate(Date date) {
  date.format('dd-MM-yy HH:mm')
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
	processShipments([61L,62L,63L,64L])
	processShipments([151L,152L])
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------