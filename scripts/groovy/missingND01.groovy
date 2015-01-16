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
csv = new File('/tmp/novaduque.csv'); csv.write('')  //Archivo de salida con los resultados

log = { text -> out << text + "\n" }
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
				                            "gte": "2014-06-01T00:00:00.000-04:00"
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
				                        "shipment.status": "shipped"
				                    }
				                }
				            ],
				            "must_not": [{"constant_score":{"filter":{"missing":{"field":"shipment.tracking_number"}}}},
				            			 {"constant_score":{"filter":{"missing":{"field":"shipment.date_created"}}}}],
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

def processShipments = { serviceId ->
	int offset = 0
	int LIMIT = 5000

	def data = getData(offset, LIMIT, serviceId)
	log "Getting ${data.total} shipments from service $serviceId"

	calculate(data.shipments)
	offset += LIMIT
	log "Processed ${offset} of ${data.total}"
	
	while (offset < data.total) {
	  sleep(1000)
	  calculate(getData(offset, LIMIT, serviceId).shipments)
	  offset += LIMIT
	  log "Processed ${offset} of ${data.total}"
	}
}

def calculate(def ships) {
	withPool() {
		ships.eachParallel { s ->
			def events = TrackingEvents.withCriteria {
				eq 'trackingNumber', s.tracking_number
				'in' 'trackingStatus', ["ND12","ND13","ND01"]
				ge 'eventDate', parseDate(s.date_created)
			}*.trackingStatus

			if (events && !("ND01" in events)) {
				addTn(s.tracking_number)
			}
		}
	}
}

synchronized void addTn(trackingNumber) {
	csv << "${trackingNumber}\n"
}

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}

// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	csv << "Tracking\n"
	
	[21L,22L,23L].each { servId ->
		processShipments(servId)
	}
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------