import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import static groovyx.gpars.GParsPool.withPool
import java.util.concurrent.atomic.AtomicInteger
import org.hibernate.FetchMode as FM
import static mercadoenvios.constants.ShippingConstants.*

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log
synchronized void log(text) { out << text + "\n" }
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
                    "query": {
				        "bool": {
				            "must": [
				                {
				                    "range": {
				                        "shipment.date_created": {
				                            "gte": "2014-01-01T00:00:00.000-04:00"
				                        }
				                    }
				                },
				                {
				                    "term": {
				                        "shipment.mode": "me2"
				                    }
				                },
				                {
				                	"query_string": {
				                		"default_field": "shipment.status",
				                        "query": "delivered not_delivered"
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
				                }
				            ],
				            "must_not": [],
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
	
	withPool(4) {
		ships.eachParallel{ s ->
			def shipment
			try {
				Shipment.withTransaction {
					shipment = Shipment.createCriteria().get {
						eq 'id', s.id
						fetchMode 'trackings', FM.JOIN
					}
					if (shipment) {
						def shippedEvents = shipment.trackings?.findAll{
							trackingStatusService.getTrackingStatus(it.trackingStatus, shipment.serviceId)?.normalizedStatus == SHIP_STATUS_SHIPPED
						}
						def shippedEvent = null
						if (shippedEvents)
							shippedEvent = shippedEvents.sort{it.eventDate}.first()

						if (shippedEvent) {
							shipment.dateShipped = shippedEvent.eventDate
							shipment.save(failOnError: true)
							log "id: ${shipment.id} tn: ${shipment?.trackingNumber} -> dateShipped: ${shippedEvent?.eventDate}"	
						}
					}
				}
				
			} catch (Exception e) {
				log "Exception ${shipment?.id}: - ${e.getMessage()} - ${e.getStackTrace()}"
			}
			
		}
	}

}

// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	
	process(getShipments())

} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------