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
                                          "gte": "2014-06-01T00:00:00.000-04:00"
                                      }
                                  }
                              },
                              {
                                  "query_string": {
                                      "default_field": "shipment.status",
                                      "query": "shipped delivered"
                                  }
                              }
                          ],
                          "must_not": [
                              {
                                  "range": {
                                      "shipment.date_created": {
                                          "from": "2014-05-12T00:00:00.000-04:00",
                                          "to": "2014-05-26T00:00:00.000-04:00"
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

out = new File('/tmp/out.log'); out.write('')
log = { text -> out << text + "\n" }
getTime = { init , end -> def time; use(groovy.time.TimeCategory) { time = end - init } }

trackingStatusService = ctx.getBean('trackingStatusService')

int offset = 0
int LIMIT = 500

events6 = Collections.synchronizedList(new ArrayList())
result2 = Collections.synchronizedList(new ArrayList())

def process(shipments) {
  def result = Collections.synchronizedList(new ArrayList())
  
  GParsPool.withPool(5) {
    shipments.eachParallel { ship ->
      ship = JsonUtil.cleanJsonNulls(ship)

      def events = TrackingEvents.withCriteria {
        eq 'trackingNumber', ship.tracking_number
        'in' 'trackingStatus', ["6","9"]
      }
      def event6 = events.find{it.trackingStatus == "6"}
      def event9 = events.find{it.trackingStatus == "9"}
      if (event6) {
        events6 << event6
        if (event6.eventDate < parseDate(ship.date_first_printed))
          result << [s: ship, e6: event6, e9: event9]
        else
          result2 << event6
      }
    }
  }
  result
}

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}

String formatDate(Date date) {
  ISODateTimeFormat.dateTime().print(new DateTime(date.getTime()))
}

log "Begin process..."
def data = getData(offset, LIMIT)
log "Total shipments to process: ${data.total}"

def wrong = []
def shipments = data.shipments
while (shipments) {
  wrong << process(shipments)
  sleep(1000)
  offset += LIMIT
  log "Processed ${offset < data.total ? offset : data.total} of ${data.total}"
  shipments = getData(offset, LIMIT).shipments
}

def wrongResults = wrong.sum()
log "RESULTS"
log "events6: ${events6.size()}"
log "result2: ${result2.size()}"
log "size: ${wrongResults.size()}"
wrongResults.collect{[it.s.id, it.s.tracking_number, 
  it.s.status_history.date_ready_to_ship, it.s.date_first_printed, 
  it.e6.eventDate, it.e6.dateCreated, it.e9.eventDate]}.each {
  log "$it"
}

log "End."