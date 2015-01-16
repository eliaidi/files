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
synchronized void log(text) { out << text + "\n" }
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
				                            "gte": "2014-08-01T00:00:00.000-04:00"
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
	withPool(7) { 
		try {
			ships.eachParallel{ s ->

				def shipmentMap = [id: s.id, shipped: s.status_history.date_shipped, estimated: s.shipping_option.estimated_delivery, visit: s.status_history.date_first_visit]

				if (isOnTime(shipmentMap)) {
					addOnTime(result, shipmentMap)
				} else {
					if (isDateShippedAfterEstimatedDelivery(shipmentMap))
						addDelayPickUp(result, shipmentMap)
					else
						addDelayDelivery(result, shipmentMap)
				}
					
				log "Processed ${s.id}"
			}
		} catch(Exception e) {
			log "Exception:: ${GrailsUtil.deepSanitize(e)}"
		}
	}
	if (result.onTime)
		result.onTime = result.onTime.sort{it.visit}
	if (result.delayPickUp)
		result.delayPickUp = result.delayPickUp.sort{it.visit}
	if (result.delayDelivery)
		result.delayDelivery = result.delayDelivery.sort{it.visit}

	return result
}

def isOnTime(shipmentMap) {
	def timeFrom = DateUtil.parseISODate(shipmentMap.estimated.time_from)
	def timeTo = DateUtil.parseISODate(shipmentMap.estimated.time_to)
	def visitDate = DateUtil.parseISODate(shipmentMap.visit)
	
	timeFrom <= visitDate && visitDate < timeTo
}

def isDateShippedAfterEstimatedDelivery(shipmentMap) {
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

synchronized void addDelayPickUp(result, shipmentMap) {
	if (!result.delayPickUp)
		result.delayPickUp = []
	result.delayPickUp << shipmentMap
}

synchronized void addDelayDelivery(result, shipmentMap) {
	if (!result.delayDelivery)
		result.delayDelivery = []
	result.delayDelivery << shipmentMap
}

void saveToFile(map){
	def moto = new File('/tmp/moto.log')
	moto.write('')
	def total = map.onTime.size() + map.delayPickUp.size() + map.delayDelivery.size()
	moto << "ON TIME: ${map.onTime?.size()}\n (${percentage(total, map.onTime.size())})"
	map.onTime.each{ moto << "$it\n" }
	def totalDelays = map.delayPickUp.size() + map.delayDelivery.size()
	moto << "DELAY PICKUP: ${map.delayPickUp?.size()}\n - (${percentage(total, map.delayPickUp.size())}) - (${percentage(totalDelays, map.delayPickUp.size())})"
	map.delayPickUp.each{ moto << "$it\n"}
	moto << "DELAY DELIVERY: ${map.delayDelivery?.size()}\n - (${percentage(total, map.delayDelivery.size())}) - (${percentage(totalDelays, map.delayDelivery.size())})"
	map.delayDelivery.each{ moto << "$it\n"}
}

def percentage(total, n) {
	return n*100/total
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