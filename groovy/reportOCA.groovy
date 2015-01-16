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
csv = new File('/tmp/timesMLASuc.csv'); csv.write('')  //Archivo de salida con los resultados

log = { text -> out << text + "\n" }
mongo = ctx.getBean('mongoService') //Para obtener speed original de oca
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService') //Para calcular los días hábiles
siteEstimatedDeliveryService = ctx.getBean('siteEstimatedDeliveryService') //Para recalcular estimated delivery
trackingStatusService = ctx.getBean('trackingStatusService')

rulesByServices = [
	61L: "rulesMLA",
	62L: "rulesMLA",
	63L: "rulesMLA",
	64L: "rulesMLA",
	151L: "agenciesRulesMLA",
	152L: "agenciesRulesMLA"
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
				                            "from": "2014-08-01T00:00:00.000-04:00",
				                            "to": "2014-09-01T00:00:00.000-04:00"
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
				            "must_not": [
				            	{
				                    "constant_score": {
				                        "filter": {
				                            "missing": {
				                                "field": "shipment.status_history.date_first_visit"
				                            }
				                        }
				                    }
				                }
				            ],
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
def getShipments = { serviceId ->
	int offset = 0
	int LIMIT = 5000

	def data = getData(offset, LIMIT, serviceId)
	log "Getting ${data.total} shipments from service $serviceId"

	def shipments = data.shipments
	offset += LIMIT
	
	while (offset < data.total) {
	  shipments << getData(offset, LIMIT, serviceId).shipments
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
	shipments.flatten()
}

def getWaitingForWithdrawalEventDate(trackingNumber, serviceId) {
	def events = TrackingEvents.findAllByTrackingNumber(trackingNumber).findAll{ 
		trackingStatusService.getTrackingStatus(it.trackingStatus, serviceId).substatus == ShipmentSubstatus.WAITING_FOR_WITHDRAWAL.id
	}
	return events ? events.sort{it.eventDate}.first().eventDate : null
}

/** Calcula tiempos de diferencia entre shipping y fecha de entrega (en días hábiles)*/
def calculate(def ships, def serviceId) {
	log "calculating..."
	def rulesCollection = rulesByServices[serviceId]
	def shipmentsAndTimes
	withPool() {
		shipmentsAndTimes = ships.collectParallel {
			
			def rule = mongo.findOne(rulesCollection, ['prices.rule_id': it.shipping_option.id])
			def carrierFinishTime = serviceId in [151L,152L] ? getWaitingForWithdrawalEventDate(it.tracking_number, serviceId) : parseDate(it.status_history.date_first_visit)
			int speed = serviceId in [151L,152L] ? rule.speed : rule.speed_from_ws
			
			def dateShipped = parseDate(it.status_history.date_shipped)	
			if ( serviceId in [61L,62L,151L] || 
				(serviceId in [63L,64L,152L] && ((dateShipped.format('HH') as int) >= 9)) ) {
				speed++
			}
				
			if (rule && carrierFinishTime) {
				def estimatedTime = siteEstimatedDeliveryService.getDeliveryDate(
									toCalendar(dateShipped),
									"MLA", 
									speed
								).getTime()
				[
					time: computeHandlingTimeService.getWorkingDays(
						estimatedTime,
						carrierFinishTime,
						it.site_id
					),
					shipment: it
				]
			}
		}
	}
	log "shipmentsAndTimes: ${shipmentsAndTimes.size()}"
	shipmentsAndTimes = shipmentsAndTimes.findAll{it}
	log "shipmentsAndTimes: ${shipmentsAndTimes.size()}"
	def times = shipmentsAndTimes*.time
	def shipments = shipmentsAndTimes*.shipment

	def onTime = times.findAll{it <= 0}
	def _1day = times.findAll{it == 1}
	def _2day = times.findAll{it == 2}
	def _3day = times.findAll{it == 3}
	def _4day = times.findAll{it == 4}
	def _5day = times.findAll{it == 5}
	def _more = times.findAll{it > 5}
	
	[shipments: shipments, onTime: onTime, _1day: _1day, _2day: _2day, _3day: _3day, _4day: _4day, _5day: _5day, _more: _more]
}

/** Procesa shipments y genera fila de resultados en archivo de salida */
def createRow = { shipments, serviceId -> 
	def firstCalc = calculate(shipments, serviceId)
	
	def columns = [
		"${serviceId},${firstCalc.shipments.size()},",
		"${firstCalc.onTime.size()},${((firstCalc.onTime.size()*100/firstCalc.shipments.size()) as float).round(2)},",
		"${firstCalc._1day.size()},${((firstCalc._1day.size()*100/firstCalc.shipments.size()) as float).round(2)},",
		"${firstCalc._2day.size()},${((firstCalc._2day.size()*100/firstCalc.shipments.size()) as float).round(2)},",
		"${firstCalc._3day.size()},${((firstCalc._3day.size()*100/firstCalc.shipments.size()) as float).round(2)},",
		"${firstCalc._4day.size()},${((firstCalc._4day.size()*100/firstCalc.shipments.size()) as float).round(2)},",
		"${firstCalc._5day.size()},${((firstCalc._5day.size()*100/firstCalc.shipments.size()) as float).round(2)},",
		"${firstCalc._more.size()},${((firstCalc._more.size()*100/firstCalc.shipments.size()) as float).round(2)}\n"
	]

	csv << columns.join()
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
	csv << "Service_id,Count,OnTime,%,1,%,2,%,3,%,4,%,5,%,5+,%\n"
	
	[61L,62L,63L,64L,151L,152L].each { servId ->
		createRow( 
			getShipments(servId),
			servId
		)
	}
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------