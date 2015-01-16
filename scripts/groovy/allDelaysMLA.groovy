import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import mercadoenvios.utils.JsonUtil
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.format.DateTimeFormatter 
import org.joda.time.DateTime
import java.math.*

/* Reporte OCA (envios - visitas)
Sucursal a puerta:
Std 61: 70.874 - 89.408
Pri 62: 8.293 - 10.703
Puerta a puerta:
Std 63: 55.148 - 68.994
Pri 64: 4.054 - 5.411
Entrega en sucursal:
SaS 151: 13 - 24
PaS 152: 1 - 3
*/

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log
csv = new File('/tmp/timesMLA.csv'); csv.write('')  //Archivo de salida con los resultados

log = { text -> out << text + "\n" }
mongo = ctx.getBean('mongoService') //Para obtener speed original de oca
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService') //Para calcular los días hábiles
siteEstimatedDeliveryService = ctx.getBean('siteEstimatedDeliveryService') //Para recalcular estimated delivery con speed de regla modificado
addressTimeHandlerService = ctx.getBean('addressTimeHandlerService') //Para excluir envíos a zonas de riesgo
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
				                            "from": "2014-07-01T00:00:00.000-04:00",
				                            "to": "2014-08-01T00:00:00.000-04:00"
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
				                                "field": "shipment.status_history.date_shipped"
				                            }
				                        }
				                    }
				                },
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

/** Filtra nulls y shipments con diferencias de tiempos exageradas */
def filterOddCases(shipmentsAndTimes) {
	shipmentsAndTimes.findAll{it && it.time >= -15 && it.time <= 15}
}

/** Calcula tiempos de diferencia entre shipping y fecha de entrega (en días hábiles)*/
def calculate(def ships) {
	def shipmentsAndTimes = ships.collect{
		def rule = mongo.findOne('rulesMLA', ['prices.rule_id': it.shipping_option.id])
		if (rule) {
			def estimatedTime = siteEstimatedDeliveryService.getDeliveryDate(
								parseCalendar(it.status_history.date_shipped),
								"MLA", 
								rule.speed_from_ws as int
							).getTime()
			[
				time: computeHandlingTimeService.getWorkingDays(
					parseDate(it.status_history.date_first_visit),
					estimatedTime,
					it.site_id
				),
				shipment: it
			]
		}
	}
	def shipmentsToProcess = filterOddCases(shipmentsAndTimes)
	def times = shipmentsToProcess*.time as double[]
	def shipments = shipmentsToProcess*.shipment

	def aboveRule = times.findAll{it > 0} as double[]
	def belowRule = times.findAll{it < 0} as double[]
	def exactRule = times.findAll{it == 0} as double[]
	
	def avgTimeDays = new DescriptiveStatistics(times).mean

	[shipments: shipments, aboveRule: aboveRule, belowRule: belowRule, exactRule: exactRule, avgTimeDays: avgTimeDays]
}

/** Procesa shipments y genera archivo de salida con resultados */
def logStats = { shipments -> 
	def firstCalc = calculate(shipments)
	
	def columns = [
		//Service_id,From,To,Count
		"${shipments[0].service_id},${shipments[0].sender_address.state.id},${shipments[0].receiver_address.state.id},${firstCalc.shipments.size()},",
		"${((new DescriptiveStatistics(firstCalc.shipments.collect{it.shipping_option.speed.shipping} as double[]).mean/24) as float).round(0)},", //Avg_speed_days
		"${firstCalc.aboveRule.size()},${((firstCalc.aboveRule.size()*100/firstCalc.shipments.size()) as float).round(2)},", //AboveRule, Percentage
		"${firstCalc.belowRule.size()},${((firstCalc.belowRule.size()*100/firstCalc.shipments.size()) as float).round(2)},", //BelowRule, Percentage
		"${firstCalc.exactRule.size()},${((firstCalc.exactRule.size()*100/firstCalc.shipments.size()) as float).round(2)},", //AboveRule, Percentage
		"${(firstCalc.avgTimeDays as float).round(2)}\n" //Avg_time_days
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
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	csv << "Service_id,From,To,Count,Avg_speed_days,Above_rule,Percentage,Below_rule,Percentage,Exact_rule,Percentage,Avg_time_days\n"
	
	[61,62,63,64].each { servId ->
		logStats( 
			getShipments(servId)
		)
	}
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------