import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import mercadoenvios.utils.JsonUtil
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.format.DateTimeFormatter 
import org.joda.time.DateTime
import java.math.*

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log
csv = new File('/tmp/delayedMLB.csv');  //Archivo de salida con los resultados
csv.write('Service_id,From,To,Count,Delayed,Percentage,Avg_speed_days,Avg_time_days,Days_added,New_delayed,New_percentage,New_avg_time_days\n')

log = { text -> out << text + "\n" }
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService') //Para calcular los días hábiles
siteEstimatedDeliveryService = ctx.getBean('siteEstimatedDeliveryService') //Para recalcular estimated delivery con speed de regla modificado
addressTimeHandlerService = ctx.getBean('addressTimeHandlerService') //Para excluir envíos a zonas de riesgo
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit, serviceId, stateIdFrom, stateIdTo ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit, serviceId, stateIdFrom, stateIdTo), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit, serviceId, stateIdFrom, stateIdTo) {
    def body = '''{
                    "query": {
				        "bool": {
				            "must": [
				                {
				                    "range": {
				                        "shipment.status_history.date_first_visit": {
				                            "from": "2014-06-23T00:00:00.000-04:00",
				                            "to": "2014-07-18T00:00:00.000-04:00"
				                        }
				                    }
				                },
				                {
				                    "term": {
				                        "shipment.site_id": "MLB"
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
				                },
				                {
				                	"term": {
				                        "shipment.sender_address.state.id": "'''+stateIdFrom+'''"
				                    }
				                },
				                {
				                	"term": {
				                        "shipment.receiver_address.state.id": "'''+stateIdTo+'''"
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
				                                "field": "shipment.status_history.date_shipped"
				                            }
				                        }
				                    }
				                },
				                {
				                    "constant_score": {
				                        "filter": {
				                            "missing": {
				                                "field": "shipment.status_history.date_delivered"
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
def getShipments = { serviceId, stateIdFrom, stateIdTo ->
	int offset = 0
	int LIMIT = 5000

	def data = getData(offset, LIMIT, serviceId, stateIdFrom, stateIdTo)
	log "Getting ${data.total} shipments from service $serviceId with $stateIdFrom -> $stateIdTo"

	def shipments = data.shipments
	offset += LIMIT
	
	while (offset < data.total) {
	  shipments << getData(offset, LIMIT, serviceId, stateIdFrom, stateIdTo).shipments
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
	shipments.flatten()
}

/** Filtra shipments con diferencias de tiempos exageradas */
def filterOddCases(shipmentsAndTimes) {
	shipmentsAndTimes.findAll{it.time >= -5 && it.time <= 15}
}

/** Calcula tiempos de diferencia entre shipping y fecha de entrega (en días hábiles)*/
def calculate(def ships, int daysToAdd = 0) {
	def shipmentsAndTimes = ships.findAll{!addressTimeHandlerService.isInRange(Integer.valueOf(it.receiver_address.zip_code))}.collect{
	def deliveryDate = siteEstimatedDeliveryService.getDeliveryDate(
							parseCalendar(it.status_history.date_shipped),
							"MLB", 
							((it.shipping_option.speed.shipping as int)/24) + daysToAdd 
						).getTime()
		[
			time: computeHandlingTimeService.getWorkingDays(
				deliveryDate,
				parseDate(it.status_history.date_first_visit),
				it.site_id
			),
			shipment: it
		]
	}
	def shipmentsToProcess = filterOddCases(shipmentsAndTimes)
	def times = shipmentsToProcess*.time as double[]
	def shipments = shipmentsToProcess*.shipment

	def delayed = times.findAll{it > 0} as double[]
	def avgTimeDays = new DescriptiveStatistics(times).mean

	[shipments: shipments, delayed: delayed, avgTimeDays: avgTimeDays]
}

/** Procesa shipments y genera archivo de salida con resultados */
def logStats = { shipments -> 
	def firstCalc = calculate(shipments, 0)
	def columns = [
		//'Service_id,From,To,Count,Delayed,
		"${shipments[0].service_id},${shipments[0].sender_address.state.id},${shipments[0].receiver_address.state.id},${firstCalc.shipments.size()},${firstCalc.delayed.size()},",
		"${((firstCalc.delayed.size()*100/firstCalc.shipments.size()) as float).round(2)},", //Percentage
		"${((new DescriptiveStatistics(firstCalc.shipments.collect{it.shipping_option.speed.shipping} as double[]).mean/24) as float).round(2)},", //Avg_speed_days
		"${(firstCalc.avgTimeDays as float).round(2)}" //Avg_time_days
	]
	def newColumns = ["\n"]

	//Si la diferencia promedio entre shipping y entrega es mayor a 1, se recalculan tiempos con shipping modificados
	if (firstCalc.avgTimeDays.abs() >= 1) {
		def daysToAdd = (firstCalc.avgTimeDays as BigDecimal).setScale(0, firstCalc.avgTimeDays > 0 ? RoundingMode.FLOOR : RoundingMode.CEILING) as int //Días que se agregan o sacan
		def secondCalc = calculate(shipments, daysToAdd)
		newColumns = [ 
			",${daysToAdd},", //Days_added
			"${secondCalc.delayed.size()},", //New_delayed
			"${((secondCalc.delayed.size()*100/secondCalc.shipments.size()) as float).round(2)},", //New_percentage
			"${(secondCalc.avgTimeDays as float).round(2)}\n" //New_avg_time_days
		]
	}
	csv << (columns + newColumns).flatten().join()
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
	[21,22,23].each { servId ->
		GroovyCollections.combinations([["BR-SP","BR-RJ","BR-MG"],["BR-SP","BR-RJ","BR-MG"]]).each{ states ->
			logStats( 
				getShipments(servId, states.first(), states.last()) 
			)
		}
	}
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------