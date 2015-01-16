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
csv = new File('/tmp/timesOcaOCT.csv'); csv.write('')  //Archivo de salida con los resultados

log = { text -> out << text + "\n" }
rulesMongoMLAService = ctx.getBean('rulesMongoMLAService')
rulesMongoMLAAgenciesService = ctx.getBean('rulesMongoMLAAgenciesService')
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService') //Para calcular los días hábiles
siteEstimatedDeliveryService = ctx.getBean('siteEstimatedDeliveryService') //Para recalcular estimated delivery
trackingStatusService = ctx.getBean('trackingStatusService')

calcAcum = resetCalc()

rulesByServices = [
	61L: [ruleService: rulesMongoMLAService, field: "speed_from_ws", eventsFrom: ["6","9"], eventsTo: ["15","16"]],
	62L: [ruleService: rulesMongoMLAService, field: "speed_from_ws", eventsFrom: ["6","9"], eventsTo: ["15","16"]],
	63L: [ruleService: rulesMongoMLAService, field: "speed_from_ws", eventsFrom: ["9","92"], eventsTo: ["15","16"]],
	64L: [ruleService: rulesMongoMLAService, field: "speed_from_ws", eventsFrom: ["9","92"], eventsTo: ["15","16"]],
	151L: [ruleService: rulesMongoMLAAgenciesService, field: "speed", eventsFrom: ["6","9"], eventsTo: ["13","20","23"]],
	152L: [ruleService: rulesMongoMLAAgenciesService, field: "speed", eventsFrom: ["9","92"], eventsTo: ["13","20","23"]]
]
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit, serviceId ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit, serviceId), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit, serviceId) {
    def body = '''{
                    "query": {
				        "bool": {
				            "must": [
				                {
				                    "range": {
				                        "shipment.status_history.date_shipped": {
				                            "from": "2014-10-01T08:00:00.000-04:00",
				                            "to": "2014-10-30T20:00:00.000-04:00"
				                        }
				                    }
				                },
				                {
				                    "range": {
				                        "shipment.status_history.date_first_visit": {
				                            "gte": "2014-10-01T08:00:00.000-04:00"
				                        }
				                    }
				                },
				                {
				                    "term": {
				                        "shipment.site_id": "MLA"
				                    }
				                },
				                {
				                	"term": {
				                        "shipment.service_id": "'''+serviceId+'''"
				                    }
				                },
				                {
				                    "term": {
				                        "shipment.mode": "me2"
				                    }
				                }
				            ],
				            "must_not": [{"constant_score":{"filter":{"missing":{"field":"shipment.tracking_number"}}}}],
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

/** Obtiene los shipments de elastic, procesando pagina por pagina. Devuelve lista completa de shipments */
def processShipments = { serviceId ->
	int offset = 0
	int LIMIT = 1000

	def data = getData(offset, LIMIT, serviceId)
	log "Getting ${data.total} shipments from service $serviceId"

	calculate(data.shipments, serviceId)
	log "Processed ${offset} of ${data.total}"
	offset += LIMIT
	
	while (offset < data.total) {
	  sleep(5000)
	  calculate(getData(offset, LIMIT, serviceId).shipments, serviceId)
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
}

def resetCalc() {
	return [shipments: [], onTime: [], _1day: [], _2day: [], _3day: [], _4day: [], _5day: [], _more: []]
}

def getFirstEventFrom(def events, def trackingNumber, def serviceId) {
	def evts = events.collect { e ->
		TrackingEvents.findByTrackingNumberAndTrackingStatus(trackingNumber, e)
	}.findAll{it}
	
	return evts ? evts.sort{it.eventDate}.first().eventDate : null
}

/** Calcula tiempos de diferencia entre shipping y fecha de entrega (en días hábiles)*/
def calculate(def ships, def serviceId) {
	log "calculating..."
	def params = rulesByServices[serviceId]
	def shipmentsAndTimes
	withPool(3) {
		shipmentsAndTimes = ships.collectParallel {
			
			def rule = params.ruleService.getRule(it.shipping_option.id as long)
			def carrierBeginTime = getFirstEventFrom(params.eventsFrom, it.tracking_number, serviceId)
			def carrierFinishTime = getFirstEventFrom(params.eventsTo, it.tracking_number, serviceId)
							
			if (rule && carrierBeginTime && carrierFinishTime) {
				int speed = rule[(params.field)]
				def estimatedTime = siteEstimatedDeliveryService.getDeliveryDate(
									toCalendar(carrierBeginTime),
									"MLA", 
									++speed
								).getTime()
				[
					time: computeHandlingTimeService.getWorkingDays(
						estimatedTime,
						carrierFinishTime,
						"MLA"
					),
					shipment: it
				]
			}
		}
	}
	log "shipmentsAndTimes: ${shipmentsAndTimes.size()}"
	shipmentsAndTimes = shipmentsAndTimes.findAll{it}
	log "shipmentsAndTimes: ${shipmentsAndTimes.size()}"
	
	def onTime = shipmentsAndTimes.findAll{it.time <= 0}
	def _1day = shipmentsAndTimes.findAll{it.time == 1}
	def _2day = shipmentsAndTimes.findAll{it.time == 2}
	def _3day = shipmentsAndTimes.findAll{it.time == 3}
	def _4day = shipmentsAndTimes.findAll{it.time == 4}
	def _5day = shipmentsAndTimes.findAll{it.time == 5}
	def _more = shipmentsAndTimes.findAll{it.time > 5}
		
	def calc = [shipments: shipmentsAndTimes*.shipment, onTime: onTime*.time, _1day: _1day*.time, _2day: _2day*.time, _3day: _3day*.time, _4day: _4day*.time, _5day: _5day*.time, _more: _more*.time]
	addCalc(calc)
}

void addCalc(calc) {
	calcAcum.shipments.addAll(calc.shipments)
	calcAcum.onTime.addAll(calc.onTime)
	calcAcum._1day.addAll(calc._1day)
	calcAcum._2day.addAll(calc._2day)
	calcAcum._3day.addAll(calc._3day)
	calcAcum._4day.addAll(calc._4day)
	calcAcum._5day.addAll(calc._5day)
	calcAcum._more.addAll(calc._more)
}

/** Procesa shipments y genera fila de resultados en archivo de salida */
void createRow(calc, serviceId) { 
	
	def columns = [
		"${serviceId},${calc.shipments.size()},",
		"${calc.onTime.size()},${((calc.onTime.size()*100/calc.shipments.size()) as float).round(2)},",
		"${calc._1day.size()},${((calc._1day.size()*100/calc.shipments.size()) as float).round(2)},",
		"${calc._2day.size()},${((calc._2day.size()*100/calc.shipments.size()) as float).round(2)},",
		"${calc._3day.size()},${((calc._3day.size()*100/calc.shipments.size()) as float).round(2)},",
		"${calc._4day.size()},${((calc._4day.size()*100/calc.shipments.size()) as float).round(2)},",
		"${calc._5day.size()},${((calc._5day.size()*100/calc.shipments.size()) as float).round(2)},",
		"${calc._more.size()},${((calc._more.size()*100/calc.shipments.size()) as float).round(2)}\n"
	]

	csv << columns.join()
}

//Funciones útiles para tratar las fechas
Calendar toCalendar(Date date) {
	def cal = Calendar.instance
	cal.setTime(date)
	cal
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	csv << "Service_id,Count,OnTime,%,1,%,2,%,3,%,4,%,5,%,5+,%\n"
	[61L,62L,63L,64L,151L,152L].each { servId ->
		processShipments(servId)
		createRow(calcAcum, servId)
		calcAcum = resetCalc()
	}
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------