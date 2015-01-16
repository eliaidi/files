import static mercadoenvios.constants.ShippingConstants.*
import org.apache.http.conn.params.ConnRoutePNames
import grails.converters.JSON
import groovyx.gpars.GParsPool
import mercadoenvios.*
import mercadoenvios.utils.JsonUtil
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

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
                                          "gte": "2014-04-01T00:00:00.000-04:00"
                                      }
                                  }
                              },
                              {
                                  "term": {
                                      "shipment.status": "not_delivered"
                                  }
                              },
                              {
                                  "query_string": {
                                      "default_field": "shipment.substatus",
                                      "query": "returning_to_sender returned stolen"
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

def createFilter(tn, rtn) {
  def filter = []
  if (tn)
    filter << tn
  if (rtn)
    filter << rtn
  return filter
}

out = new File('/tmp/out.log'); out.write('')
log = { text -> out << text + "\n" }
getTime = { init , end -> def time; use(groovy.time.TimeCategory) { time = end - init } }

trackingStatusService = ctx.getBean('trackingStatusService')
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService')

int offset = 0
int LIMIT = 500

void addHours(map, prop, hours) {
  if (map[(prop)]) {
    map[(prop)] << hours
  } else {
    map[(prop)] = []
    map[(prop)] << hours
  }
}

process = { shs, result, prop ->
  shs.each { ship ->
    //log "shipment ${ship.id}"
    ship = JsonUtil.cleanJsonNulls(ship)

    def events = TrackingEvents.withCriteria {
      'in' 'trackingNumber', createFilter(ship.tracking_number, ship.return_tracking_number)
    }
    def eventsWithStatus = events.collect{ event ->
      def trackingStatus = trackingStatusService.getTrackingStatus(event.trackingStatus, ship.service_id) 
      [event: event, normalizedStatus: trackingStatus.normalizedStatus, normalizedSubstatus: trackingStatus.substatus]
    }
    //log "eventsWithStatus: $eventsWithStatus"
    def returningToSenderEvents = eventsWithStatus.findAll{
      it.normalizedStatus == SHIP_STATUS_NOT_DELIVERED && 
      it.normalizedSubstatus == SHIP_SUB_STATUS_RETURNING_TO_SENDER}.sort{it.event.eventDate}

    def returnedEvents = eventsWithStatus.findAll{
      it.normalizedStatus == SHIP_STATUS_NOT_DELIVERED &&
      it.normalizedSubstatus == SHIP_SUB_STATUS_RETURNED}.sort{it.event.eventDate}
    //log "returningToSenderEvents: $returningToSenderEvents"
    //log "returnedEvents: $returnedEvents"
    if (returningToSenderEvents && returnedEvents) {
      def diff = computeHandlingTimeService.getWorkingDays(returningToSenderEvents.first().event.eventDate, returnedEvents.first().event.eventDate, "MLB")
      if (diff >= 0)
        addHours(result, prop, diff)
    }
  }
}

def processReturns(ships, property1, property2 = null, property3 = null) {
  def result = Collections.synchronizedMap(new HashMap<String, List<Integer>>())
  GParsPool.withPool(10) {
    if (property3) {
      ships.groupBy{ "${it[(property1)][(property3)]}-${it[(property2)][(property3)]}" }.eachParallel { prop, shs ->
        process(shs, result, prop)
      }
    } else if (property2)
      ships.groupBy{it[(property1)][(property2)]}.eachParallel { prop, shs ->
        process(shs, result, prop)
      }
    else
      ships.groupBy{it[(property1)]}.eachParallel { prop, shs ->
        process(shs, result, prop)
      }
  }
  result
}

def processByState(ships) {
  def result = Collections.synchronizedMap(new HashMap<String, List<Integer>>())
  GParsPool.withPool(10) {
    ships.groupBy{[it.sender_address.state.id, it.receiver_address.state.id]}.eachParallel { prop, shs ->
      process(shs, result, prop)
    }
  }
  result
}

def processStolenReturns(ships) {
  def result = Collections.synchronizedSet(new HashSet())
  def total = Collections.synchronizedSet(new HashSet())
  GParsPool.withPool(10) {
    ships.eachParallel { ship ->
      ship = JsonUtil.cleanJsonNulls(ship)

      def events = TrackingEvents.withCriteria {
        'in' 'trackingNumber', createFilter(ship.tracking_number, ship.return_tracking_number)
      }
      def eventsWithStatus = events.collect{ event ->
        def trackingStatus = trackingStatusService.getTrackingStatus(event.trackingStatus, ship.service_id) 
        [event: event, normalizedStatus: trackingStatus.normalizedStatus, normalizedSubstatus: trackingStatus.substatus]
      }
      //log "eventsWithStatus: $eventsWithStatus"
      def returningToSenderEvents = eventsWithStatus.findAll{
        it.normalizedStatus == SHIP_STATUS_NOT_DELIVERED && 
        it.normalizedSubstatus == SHIP_SUB_STATUS_RETURNING_TO_SENDER}.sort{it.event.eventDate}

      def stolenEvents = eventsWithStatus.findAll{
        it.normalizedStatus == SHIP_STATUS_NOT_DELIVERED &&
        it.normalizedSubstatus == "stolen"}.sort{it.event.eventDate}
      //log "returningToSenderEvents: $returningToSenderEvents"
      //log "stolenEvents: $stolenEvents"
      if (returningToSenderEvents && stolenEvents)
        result << ship
      total << ship
    }
  }
  [stolen: result, total: total]
}

def orderMapByProperties = { map ->
  map.collect{it.entrySet().key}.flatten().unique().collect{ k -> [(k): map.collect{ it[(k)] }.findAll{it}.flatten()] }.sum()
}

log "Begin process..."

def data = getData(offset, LIMIT)
log "Total shipments to process: ${data.total}"

def hoursByService = []; def hoursByRule = []; def hoursByZipCodes = []; def stolenReturns = []; hoursByState = []

def shipments = data.shipments
while (shipments) {
  hoursByService << processReturns(shipments, "service_id")
  sleep(1000)
  hoursByState << processByState(shipments)
  sleep(1000)
  stolenReturns << processStolenReturns(shipments)
  sleep(1000)

  offset += LIMIT
  log "Processed ${offset < data.total ? offset : data.total} of ${data.total}"
  shipments = getData(offset, LIMIT).shipments

}
def resultHoursByService = orderMapByProperties(hoursByService)
def resultHoursByRule = orderMapByProperties(hoursByRule)
def resultHoursByZipCodes = orderMapByProperties(hoursByZipCodes)
def resultStolenReturns = [total: stolenReturns.collect{it.total}.flatten(), stolen: stolenReturns.collect{it.stolen}.flatten()]
def resultHoursByState = hoursByState.sum()

def calculate75(allHours) {
  log "size: ${allHours.size()}"
  def p90 = new DescriptiveStatistics(allHours as double[]).getPercentile(90)
  allHours = allHours.findAll{it <= p90}
  log "new size: ${allHours.size()}"
  
  def mean = allHours.sum()/allHours.size()
  def below = allHours.findAll{it < mean}.size()
  def percent = below*100/allHours.size()
  
  log "mean: $mean"
  log "below: $below"
  log "percent: $percent%"

  def meetBelow = allHours.size()*75/100
  def newMean = mean++
  def newBelow = allHours.findAll{it < newMean}.size()
  while (newBelow < meetBelow){
    newMean++
    newBelow = allHours.findAll{it < newMean}.size()
  }
  def newPercent = newBelow*100/allHours.size()

  log "meetBelow: $meetBelow"
  log "newMean: $newMean"
  log "newBelow: $newBelow"
  log "newPercent: $newPercent%"
}

log "RESULTS"
if (resultHoursByService) {
  log "-------- by services ----------"
  resultHoursByService.sort{it.key}.each { servId, hs ->
    log "$servId: ${hs.sum()/hs.size()}"
    calculate75(hs)
  }
  def allHours = resultHoursByService.collect{it.value}.flatten()
  calculate75(allHours)
}
if (resultHoursByRule) {
  log "-------- by rules ----------"
  resultHoursByRule.sort{it.key}.each { ruleId, hs ->
    log "$ruleId: ${hs.sum()/hs.size()}"
    log "hs: $hs"  
  }
}
if (resultHoursByZipCodes) {
  log "-------- by zip_codes ----------"
  resultHoursByZipCodes.sort{it.key}.each { zipcodes, hs ->
    log "$zipcodes: ${hs.sum()/hs.size()}"
    log "hs: $hs"  
  }
}
if (resultHoursByState) {
  log "-------- by states -------------"
  resultHoursByState.sort{a, b -> b.value.size() <=> a.value.size()}.each { states, hs ->
    log "$states: ${hs.sum()/hs.size()}"
    log "hs: $hs"
  }
}
if (resultStolenReturns.total.size()) {
  log "-------- stolen ----------"
  log "total returns: ${resultStolenReturns.total.size()}"
  log "stolen: ${resultStolenReturns.stolen.size()} - ids: ${resultStolenReturns.stolen*.id}"
}
if (!resultHoursByService && !resultHoursByRule && !resultHoursByZipCodes && !stolenReturns)
  log "No results found"
log "End."