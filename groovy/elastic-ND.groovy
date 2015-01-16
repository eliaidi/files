import static mercadoenvios.constants.ShippingConstants.*
import org.apache.http.conn.params.ConnRoutePNames
import grails.converters.JSON
import groovyx.gpars.GParsPool
import mercadoenvios.*
import mercadoenvios.utils.JsonUtil
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import java.util.concurrent.atomic.AtomicInteger

restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)

getData = { offset, limit ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

computeHandlingTimeService = ctx.getBean('computeHandlingTimeService')

def getQuery(offset, limit) {
    def body = '''{
                   "query": {
                      "bool": {
                          "must": [
                              {
                                  "term": {
                                      "shipment.site_id": "MLB"
                                  }
                              },
                              {
                                  "range": {
                                      "shipment.service_id": {
                                          "from": "21",
                                          "to": "23"
                                      }
                                  }
                              },
                              {
                                  "range": {
                                      "shipment.date_created": {
                                          "gte": "2014-05-01T00:00:00.000-04:00"
                                      }
                                  }
                              },
                              {
                                  "query_string": {
                                      "default_field": "shipment.status",
                                      "query":"delivered not_delivered"
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
getTime = { init , end -> def time; use(groovy.time.TimeCategory) { time = end - init } }

trackingStatusService = ctx.getBean('trackingStatusService')

def orderMapByProperties = { map ->
  map.collect{it.entrySet().key}.flatten().unique().collect{ k -> [(k): map.collect{ it[(k)] }.findAll{it}.flatten()] }.sum()
}

int offset = 0
int LIMIT = 250

void addHours(map, prop, hours) {
  if (map[(prop)]) {
    map[(prop)] << hours
  } else {
    map[(prop)] = []; map[(prop)] << hours
  }
}

negDeli = new AtomicInteger(0)
negNotDeli = new AtomicInteger(0)
noPi = new AtomicInteger(0)
notLost = new AtomicInteger(0)
samplesDeli = Collections.synchronizedList(new ArrayList())
samplesNotDeli = Collections.synchronizedList(new ArrayList())

def processShipments(ships) {
  def result = Collections.synchronizedMap(new HashMap<String, List<Integer>>())
  def allEvents
  try {
    ships = JsonUtil.cleanJsonNulls(ships)
    allEvents = TrackingEvents.withCriteria {
      'in' 'trackingNumber', ships*.tracking_number
      'in' 'trackingStatus', ["BDE09","BDI09","BDR09","ND10","ND01","ND03"]
    }
      
    //GParsPool.withPool {
      allEvents.groupBy{it.trackingNumber}.each { tn, events ->
        def piEvent = events.find{it.trackingStatus == "ND10"}
        def lostEvents = events.findAll{it.trackingStatus in ["BDE09","BDI09","BDR09"]}
        def deliveredNovaDuqueEvent = events.find{it.trackingStatus == "ND01"}
        def notDeliveredNovaDuqueEvent = events.find{it.trackingStatus == "ND03"}

        if (lostEvents) {
          if (piEvent) {
            if (deliveredNovaDuqueEvent) {
              def diff = computeHandlingTimeService.getWorkingDays(piEvent.dateCreated, deliveredNovaDuqueEvent.dateCreated, "MLB")
              if (diff >= 0) {
                addHours(result, SHIP_STATUS_DELIVERED, diff)
                samplesDeli << [pi: piEvent.dateCreated, deli: deliveredNovaDuqueEvent.dateCreated]
              } else
                negDeli.getAndIncrement()
            }
            if (notDeliveredNovaDuqueEvent) {
              def diff = computeHandlingTimeService.getWorkingDays(piEvent.dateCreated, notDeliveredNovaDuqueEvent.dateCreated, "MLB")
              if (diff >= 0) {
                addHours(result, SHIP_STATUS_NOT_DELIVERED, diff)
                samplesNotDeli << [pi: piEvent.dateCreated, notDeli: notDeliveredNovaDuqueEvent.dateCreated]
              } else
                negNotDeli.getAndIncrement()
            }
          } else {
            noPi.getAndIncrement()
          }
        } else {
          notLost.getAndIncrement()
        }
      }
    //}
  } catch (Exception e) {
    log "Exception: ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
/*    log "allEvents.size(): ${allEvents.size()}"
    if (allEvents.size() > 0) {
      log "allEvents[0]: ${allEvents[0].dump()}"
      log "--allEvents--begin---"
      allEvents.groupBy{it.trackingNumber}.each{
        log "${it}"
      }
      log "--allEvents--end-----"
    }*/
  }
  result
}

log "Begin process..."
def data = getData(offset, LIMIT)
log "Total shipments to process: ${data.total}"

def hoursByStatus = []
def shipments = data.shipments
while (shipments) {
  hoursByStatus << processShipments(shipments)
  sleep(100)
  offset += LIMIT
  log "Processed ${offset < data.total ? offset : data.total} of ${data.total}"
  shipments = getData(offset, LIMIT).shipments
}
//log "hoursByStatus: ${hoursByStatus}"
def resultHoursByStatus = orderMapByProperties(hoursByStatus)
//log "resultHoursByStatus: ${resultHoursByStatus.collect{[it.key, it.value.size()]}}"

log "RESULTS"
log "notLost: ${notLost.get()}"
log "noPi: ${noPi.get()}"
log "negDeli: ${negDeli.get()}"
log "negNotDeli: ${negNotDeli.get()}"
try {
  if (resultHoursByStatus) {
    log "-------- by status ----------"
    resultHoursByStatus.each { status, hs ->
      if (hs.size() > 0)
        log "$status: ${hs.sum()/hs.size()} - ${hs.size()}"
      else
        log "$status: ${hs.size()}"
    }
    log "-- samplesDeli --"
    samplesDeli.each { log "$it" }
    log "-- samplesNotDeli --"
    samplesNotDeli.each { log "$it" }
    //def allHours = resultHoursByStatus.collect{it.value}.flatten()
    //log "all: ${allHours.sum()/allHours.size()} - ${allHours.size()}"

  } else
    log "No results found"
} catch (Exception e) {
  log "Exception: ${e.getMessage()} - ${e.getCause()} - ${e.getStrackTrace()}"
}

log "End."