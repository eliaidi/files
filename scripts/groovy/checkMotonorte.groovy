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
				                            "gte": "2014-06-01T00:00:00.000-04:00"
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

				if (isSameDay(shipmentMap)) {
					if (isOnTime(shipmentMap))
						addOnTime(result, shipmentMap)
					else
						addDelayedSameDay(result, shipmentMap)
				} else {
					if (wouldNotify(shipmentMap))
						addDelayedWithNotif(result, shipmentMap)
					else
						addDelayedNoNotif(result, shipmentMap)
				}
					
				log "Processed ${s.id}"
			}
		} catch(Exception e) {
			log "Exception:: ${GrailsUtil.deepSanitize(e)}"
		}
	}
	result.onTime = result.onTime.sort{it.visit}
	if (result.delayedSameDay)
		result.delayedSameDay = result.delayedSameDay.sort{it.visit}
	if (result.delayedWithNotif)
		result.delayedWithNotif = result.delayedWithNotif.sort{it.visit}
	if (result.delayedNoNotif)
		result.delayedNoNotif = result.delayedNoNotif.sort{it.visit}

	return result
}

def isSameDay(shipmentMap) {
	def estimatedDate = DateUtil.parseISODate(shipmentMap.estimated.date)
	def visitDate = DateUtil.parseISODate(shipmentMap.visit)
	def estCal = Calendar.instance; estCal.setTime(estimatedDate)
	def visCal = Calendar.instance;	visCal.setTime(visitDate)

	estCal.add(Calendar.HOUR, 1) //Porque las fechas estimadas estan con horario 00

	estCal.get(Calendar.DAY_OF_MONTH) == visCal.get(Calendar.DAY_OF_MONTH)
}

def isOnTime(shipmentMap) {
	def timeFrom = DateUtil.parseISODate(shipmentMap.estimated.time_from)
	def timeTo = DateUtil.parseISODate(shipmentMap.estimated.time_to)
	def visitDate = DateUtil.parseISODate(shipmentMap.visit)
	
	timeFrom <= visitDate && visitDate < timeTo
}

def wouldNotify(shipmentMap) {
	if (!shipmentMap.shipped)
		return true
	else {
		def calendar = Calendar.instance
		calendar.setTime(DateUtil.parseISODate(shipmentMap.estimated.time_to))
		def hour = calendar.get(Calendar.HOUR_OF_DAY)

		if (hour == 17) {
			calendar.setTime(DateUtil.parseISODate(shipmentMap.shipped))
			return calendar.get(Calendar.HOUR_OF_DAY) < 18
		} else if (hour == 12) {
			calendar.setTime(DateUtil.parseISODate(shipmentMap.shipped))
			return calendar.get(Calendar.HOUR_OF_DAY) < 13	
		} else {
			throw new Exception("Invalid time_to")
		}
	}
}

synchronized void addOnTime(result, shipmentMap) {
	if (!result.onTime)
		result.onTime = []
	result.onTime << shipmentMap
}

synchronized void addDelayedSameDay(result, shipmentMap) {
	if (!result.delayedSameDay)
		result.delayedSameDay = []
	result.delayedSameDay << shipmentMap
}

synchronized void addDelayedWithNotif(result, shipmentMap) {
	if (!result.delayedWithNotif)
		result.delayedWithNotif = []

	def v = DateUtil.parseISODate(shipmentMap.visit).getTime()/1000/60/60
	def t = DateUtil.parseISODate(shipmentMap.estimated.time_to).getTime()/1000/60/60

	def hoursDiff = ((v - t) > 0) ? ([hoursDiff: v - t]) : ([hoursDiff: 0])

	result.delayedWithNotif << shipmentMap + hoursDiff
}

synchronized void addDelayedNoNotif(result, shipmentMap) {
	if (!result.delayedNoNotif)
		result.delayedNoNotif = []
	result.delayedNoNotif << shipmentMap
}

void saveToFile(map){
	def moto = new File('/tmp/moto.log')
	moto.write('')
	moto << "ON TIME: ${map.onTime?.size()}\n"
	map.onTime.each{ moto << "$it\n" }
	moto << "DELAYED SAME DAY: ${map.delayedSameDay?.size()}\n"
	map.delayedSameDay.each{ moto << "$it\n"}

	def hoursDiff = map.delayedWithNotif?.findAll{it.hoursDiff > 0}.collect{it.hoursDiff} as double[]
	def p90 = new DescriptiveStatistics(hoursDiff).getPercentile(90)
	def filteredData = hoursDiff.findAll{it <= p90} as double[]
	def filteredStats = new DescriptiveStatistics(filteredData)
	//TODO % menor a 24 horas
	moto << "DELAYED WITH NOTIF: ${map.delayedWithNotif?.size()}\n"
	moto << "hoursDiff: ${filteredStats}\n"
	map.delayedWithNotif.each{ moto << "$it\n"}
	moto << "DELAYED NO NOTIF: ${map.delayedNoNotif?.size()}\n"
	map.delayedNoNotif.each{ moto << "$it\n"}
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