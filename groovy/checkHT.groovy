import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import static groovyx.gpars.GParsPool.withPool
import java.util.concurrent.atomic.AtomicInteger
import org.joda.time.format.ISODateTimeFormat

// ************************************************ Variables y beans *******************************************
SITE_ID = "MLB"; SERVICES = "21 22 23"
//SITE_ID = "MLA"; SERVICES = "61 62 63 64 151 152"

restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File("/tmp/out${SITE_ID}.log"); out.write('') //Archivo de log
log = { text -> out << text + "\n" }
okey = new File("/tmp/okey${SITE_ID}.log"); okey.write('')
above = new File("/tmp/above${SITE_ID}.log"); above.write('')
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService')
// ---------------------------------------------------------------------------------------------------------------

// ************************************************ Elastic *******************************************
getData = { offset, limit, services ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit, services), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit, services) {
    def body = '''{
                    "query": {"bool":{"must":[{"range":{"shipment.status_history.date_shipped":{
"gte":"2014-09-25T00:00:00.000-04:00"}}},{"term":{"shipment.mode":"me2"}},{"query_string":{"default_field":"shipment.service_id",
"query":"'''+services+'''"}}],"must_not":[{"constant_score":{"filter":{"missing":
{"field":"shipment.status_history.date_ready_to_ship"}}}},{"constant_score":{"filter":{"missing":{"field":
"shipment.shipping_option.speed.handling"}}}}],"should":[]}},
					"from": '''+offset+''',
					"size": '''+limit+''',
					"sort": [],
					"facets": {}
                }'''
}
// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Métodos *******************************************

/** Obtiene los shipments de elastic, procesando pagina por pagina. Devuelve lista completa de shipments */
def processShipments = { services ->
	int offset = 0
	int LIMIT = 15000

	def data = getData(offset, LIMIT, services)
	log "Getting ${data.total} shipments"

	process(data.shipments)
	offset += LIMIT
	
	while (offset < data.total) {
	  process(getData(offset, LIMIT, services).shipments)
	  log "Processed ${offset} of ${data.total}"

	  offset += LIMIT
	}
}

/** Calcula tiempos de diferencia entre shipping y fecha de entrega (en días hábiles)*/
def process(def ships) {
    withPool() {
		ships.eachParallel{ s ->
			def realHt = computeHandlingTimeService.getWorkingDays(parseDate(s.status_history.date_ready_to_ship), parseDate(s.status_history.date_shipped), "MLA")
            def diff = realHt*24 - Integer.valueOf(s.shipping_option.speed.handling)
            if (diff > 0)
              addShipment(above, s.id)
            else
              addShipment(okey, s.id)
		}
	}
}

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}

synchronized void addShipment(file, id) {
  file << "${id}\n"
}

// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	
	processShipments(SERVICES)

} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------