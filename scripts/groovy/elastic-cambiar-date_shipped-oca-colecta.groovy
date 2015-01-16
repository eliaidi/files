import static mercadoenvios.constants.ShippingConstants.*
import org.apache.http.conn.params.ConnRoutePNames
import grails.converters.JSON
import groovyx.gpars.GParsPool
import mercadoenvios.*
import mercadoenvios.utils.JsonUtil
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.format.DateTimeFormatter 
import org.joda.time.DateTime


restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)

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
                                  "term": {
                                      "shipment.site_id": "MLA"
                                  }
                              },
                              {
                                  "range": {
                                      "shipment.service_id": {
                                          "from": "63",
                                          "to": "64"
                                      }
                                  }
                              },
                              {
                                  "range": {
                                      "shipment.date_created": {
                                          "gte": "2014-06-25T13:45:00.000-04:00"
                                      }
                                  }
                              },
                              {
                                  "query_string": {
                                      "default_field": "shipment.status",
                                      "query": "shipped delivered not_delivered"
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

out = new File('/tmp/out.log'); out.write('')
log = { text -> out << text + "\n" }
outR = new File('/tmp/results.log'); outR.write('')
logR = { text -> outR << text + "\n" }

int offset = 0
int LIMIT = 500

void process(shipments) {
  shipments.each { ship ->
    try {
      ship = JsonUtil.cleanJsonNulls(ship)
      def event6 = TrackingEvents.findByTrackingNumberAndTrackingStatus(ship.tracking_number, "6")
      
      if (event6) {
        Shipment.withTransaction {
          def shipment = Shipment.get(ship.id)
          def previousDateShipped = shipment.dateShipped
          shipment.dateShipped = event6.eventDate  

          shipment.save()
          logR "id: ${shipment.id}, from: ${formatDate(previousDateShipped)}, to: ${formatDate(shipment.dateShipped)}, event_id: ${event6.id}"
        }
      } else {
        logR "event6 not found for shipment ${ship.id} ${ship.tracking_number}"
      }

    } catch(Exception e) {
      log "Exception: ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
    }
  }
}

String formatDate(Date date) {
  ISODateTimeFormat.dateTime().print(new DateTime(date.getTime()))
}

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}

log "Begin process..."
def data = getData(offset, LIMIT)
log "Total shipments to process: ${data.total}"

def modified = []
def shipments = data.shipments
while (shipments) {
  process(shipments)
 
  offset += LIMIT
  log "Processed ${offset < data.total ? offset : data.total} of ${data.total}"
  shipments = getData(offset, LIMIT).shipments
}

log "End."