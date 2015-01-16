import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import static groovyx.gpars.GParsPool.withPool
import java.util.concurrent.atomic.AtomicInteger
import grails.util.GrailsUtil
import mercadoenvios.utils.DateUtil
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log
delay = new File('/tmp/delay.csv'); delay.write('')
pickUpNull = new File('/tmp/pickUpNull.csv'); pickUpNull.write('')
pickUpDelayed = new File('/tmp/pickUpDelayed.csv'); pickUpDelayed.write('')
synchronized void log(text) { out << text + "\n" }
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService')
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Elastic *******************************************
getData = { offset, limit ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit) {
    def body = '''{
                    "query": {
				        "bool": {
				            "must": [
				                {
				                    "range": {
				                        "shipment.status_history.date_shipped": {
				                            "gte": "2014-09-01T00:00:00.000-04:00",
				                            "lte": "2014-10-01T00:00:00.000-04:00"
				                        }
				                    }
				                },
				                {
				                    "term": {
				                        "shipment.service_id": "81"
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
def getShipments = {
	int offset = 0
	int LIMIT = 5000

	def data = getData(offset, LIMIT)
	log "Getting ${data.total} shipments"

	def shipments = data.shipments
	offset += LIMIT
	
	while (offset < data.total) {
	  shipments.addAll(getData(offset, LIMIT).shipments)
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
	shipments
}

def process(def ships) {
	def result = Collections.synchronizedMap(new HashMap<String, List>())
	withPool() { 
		try {
			ships.eachParallel{ s ->

				def shipmentMap = [id: s.id, shipped: s.status_history.date_shipped, estimated: s.shipping_option.estimated_delivery, visit: s.status_history.date_first_visit]

				if (isOnTime(shipmentMap)) {
					addOnTime(result, shipmentMap)
				} else {
					if (isPickUpOnTime(shipmentMap))
						addDelayDelivery(result, shipmentMap)
					else {
						if (isDateShippedAfterJobRun(shipmentMap)) {
							addDelayPickUpWithDateShippedNull(result, shipmentMap)
						} else
							addDelayPickUpModified(result, shipmentMap)
					}
						
				}
					
				log "Processed ${s.id}"
			}
		} catch(Exception e) {
			log "Exception:: ${GrailsUtil.deepSanitize(e)}"
		}
	}
	if (result.onTime)
		result.onTime = result.onTime.sort{it.visit}
	if (result.delayDelivery)
		result.delayDelivery = result.delayDelivery.sort{it.visit}
	if (result.delayPickUpModified)
		result.delayPickUpModified = result.delayPickUpModified.sort{it.visit}
	if (result.delaypickupWithDateShippedNull)
		result.delaypickupWithDateShippedNull = result.delaypickupWithDateShippedNull.sort{it.visit}

	return result
}

def isOnTime(shipmentMap) {
	def timeFrom = DateUtil.parseISODate(shipmentMap.estimated.time_from)
	def timeTo = DateUtil.parseISODate(shipmentMap.estimated.time_to)
	def visitDate = DateUtil.parseISODate(shipmentMap.visit)
	
	( visitDate < timeFrom ) ||
	( (timeFrom <= visitDate) && (visitDate < (new Date(timeTo.getTime() + (1000*60*60*2)))) )
}

def isPickUpOnTime(shipmentMap) {
	def cal = Calendar.instance

	def dateShipped = DateUtil.parseISODate(shipmentMap.shipped)
	cal.setTime(dateShipped)
	def hourShipped = cal.get(Calendar.HOUR_OF_DAY)
	def dayShipped = cal.get(Calendar.DAY_OF_MONTH)

	def from = DateUtil.parseISODate(shipmentMap.estimated.time_from)
	cal.setTime(from)
	def hourEstimatedFrom = cal.get(Calendar.HOUR_OF_DAY)
	def dayEstimated = cal.get(Calendar.DAY_OF_MONTH)

	if (hourEstimatedFrom == 8) {
		return dayShipped == dayEstimated - 1 && hourShipped >= 14 && hourShipped < 22
	} else if (hourEstimatedFrom == 13) {
		return dayShipped == dayEstimated && hourShipped >= 10 && hourShipped < 14
	} else {
		log "hour=${hourEstimatedFrom}"
		return false
	}
}

def isDateShippedAfterJobRun(shipmentMap) {
	def shippedTime = DateUtil.parseISODate(shipmentMap.shipped)
	def timeTo = DateUtil.parseISODate(shipmentMap.estimated.time_to)
	timeTo = new Date(timeTo.getTime() + (1000*60*60*2))
	return shippedTime > timeTo
}

synchronized void addOnTime(result, shipmentMap) {
	if (!result.onTime)
		result.onTime = []
	result.onTime << shipmentMap
}

synchronized void addDelayPickUpModified(result, shipmentMap) {
	if (!result.delayPickUpModified)
		result.delayPickUpModified = []
	result.delayPickUpModified << shipmentMap
}

synchronized void addDelayPickUpWithDateShippedNull(result, shipmentMap) {
	if (!result.delaypickupWithDateShippedNull)
		result.delaypickupWithDateShippedNull = []

	def timeTo = DateUtil.parseISODate(shipmentMap.estimated.time_to)
	def visited = DateUtil.parseISODate(shipmentMap.visit)
	def diff = computeHandlingTimeService.getWorkingDays(timeTo, visited, "MLA") * 24
	
	result.delaypickupWithDateShippedNull << shipmentMap + [diff: diff]
}

synchronized void addDelayDelivery(result, shipmentMap) {
	if (!result.delayDelivery)
		result.delayDelivery = []
	result.delayDelivery << shipmentMap
}

void saveToFile(map){
	def moto = new File('/tmp/moto.log')
	moto.write('')
	def totalDelays = map.delayPickUpModified.size() + map.delaypickupWithDateShippedNull.size() + map.delayDelivery.size()
	def total = map.onTime.size() + totalDelays
	moto << "ON TIME: ${map.onTime?.size()} (${percentage(total, map.onTime.size())})\n"
	map.onTime.each{ moto << "$it\n" }
	moto << "DELAY PICKUP MODIFIED: ${map.delayPickUpModified?.size()} - (${percentage(total, map.delayPickUpModified.size())}) - (${percentage(totalDelays, map.delayPickUpModified.size())})\n"
	map.delayPickUpModified.each{ moto << "$it\n"}
	def dif = map.delaypickupWithDateShippedNull.collect{it.diff}
	moto << "DELAY PICKUP DS NULL: ${map.delaypickupWithDateShippedNull?.size()} - (${percentage(total, map.delaypickupWithDateShippedNull.size())}) - (${percentage(totalDelays, map.delaypickupWithDateShippedNull.size())}) - dif: ${dif.sum()/dif.size()}\n"
	map.delaypickupWithDateShippedNull.each{ moto << "$it\n"}
	moto << "DELAY DELIVERY: ${map.delayDelivery?.size()} - (${percentage(total, map.delayDelivery.size())}) - (${percentage(totalDelays, map.delayDelivery.size())})\n"
	map.delayDelivery.each{ moto << "$it\n"}

	delay << "ID, SHIPPED, TIME_FROM, TIME_TO, VISIT\n"
	map.delayDelivery.each {
		delay << "${it.id},${it.shipped},${it.estimated.time_from},${it.estimated.time_to},${it.visit}\n"
	}
	pickUpNull << "ID, SHIPPED, TIME_FROM, TIME_TO, VISIT\n"
	map.delaypickupWithDateShippedNull.each {
		pickUpNull << "${it.id},${it.shipped},${it.estimated.time_from},${it.estimated.time_to},${it.visit}\n"
	}
	pickUpDelayed << "ID, SHIPPED, TIME_FROM, TIME_TO, VISIT\n"
	map.delayPickUpModified.each {
		pickUpDelayed << "${it.id},${it.shipped},${it.estimated.time_from},${it.estimated.time_to},${it.visit}\n"
	}
}

def percentage(total, n) {
	return Math.round(n*100/total)
}

// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	
	saveToFile(process(getShipments()))

} catch (Exception e) {
	log "Exception: ${GrailsUtil.deepSanitize(e)}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------