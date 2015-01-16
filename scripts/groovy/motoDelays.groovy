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
moto = new File('/tmp/moto.csv'); moto.write('')

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
				                            "from": "2014-10-01T08:00:00.000-04:00",
				                            "to": "2014-10-23T20:00:00.000-04:00"
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
				                        "shipment.status": "delivered"
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
	int LIMIT = 100

	def data = getData(offset, LIMIT, serviceId)
	log "Getting ${data.total} shipments from service $serviceId"

	calculate(data.shipments)
	offset += LIMIT
	log "Processed ${offset} of ${data.total}"
	
	while (offset < data.total) {
	  calculate(getData(offset, LIMIT, serviceId).shipments)
	  offset += LIMIT
	  log "Processed ${offset} of ${data.total}"
	}
}

/** Calcula tiempos de diferencia entre shipping y fecha de entrega (en días hábiles)*/
def calculate(def ships) {
	log "calculating..."
	ships.each {
		def event = TrackingEvents.findByTrackingNumberAndTrackingStatus(it.tracking_number, "20")
		if (event) {
			def dayEvent = toCalendar(event.eventDate).get(Calendar.DATE)
			def dayCreated = toCalendar(event.dateCreated).get(Calendar.DATE)
			if (dayCreated > dayEvent)
				moto << "${it.tracking_number},${formatDate(event.eventDate)},${formatDate(event.dateCreated)}\n"
		}
	}
}

String formatDate(Date date) {
  date.format('dd-MM-yy HH:mm')
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
	moto << "Tracking, Entregado, Evento recibido\n"
	[81L].each { servId ->
		processShipments(servId)
	}
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------