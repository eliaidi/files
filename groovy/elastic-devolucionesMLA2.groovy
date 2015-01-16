import static mercadoenvios.constants.ShippingConstants.*
import org.apache.http.conn.params.ConnRoutePNames
import grails.converters.JSON
import mercadoenvios.utils.JsonUtil
import mercadoenvios.*
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.format.DateTimeFormatter 
import org.joda.time.DateTime
import java.math.*

restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('')
csv = new File('/tmp/returnsMLA.csv'); 
csv.write('From,To,Count,Returned,Percentage,Avg_delay_time\n')

log = { text -> out << text + "\n" }
computeHandlingTimeService = ctx.getBean('computeHandlingTimeService') //Para working days

getData = { offset, limit, from, to ->
    def res = restConn.execPost('http://shipping-elasticsearch.ml.com/shipments/_search', getQuery(offset, limit, from, to), 'json')
    def jsonHits = JSON.parse(res.data).hits
    [shipments: jsonHits.hits._source, total: jsonHits.total]
}

def getQuery(offset, limit, from, to) {
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
                                          "from": "61",
                                          "to": "64"
                                      }
                                  }
                              },
                              {
                                   "range": {
                                      "shipment.status_history.date_not_delivered": {
                                          "from": "'''+from+'''",
                                          "to": "'''+to+'''"
                                      }
                                  }
                              },
                              {
                                  "term": {
                                      "shipment.mode": "me2"
                                  }
                              },
                              {
                                  "term": {
                                      "shipment.status": "not_delivered"
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

def getShipments = { from, to ->
  int offset = 0
  int LIMIT = 500

  def data = getData(offset, LIMIT, from, to)
  log "Getting ${data.total} shipments from $from to $to"

  def shipments = data.shipments
  offset += LIMIT
  
  while (offset < data.total) {
    shipments << getData(offset, LIMIT, from, to).shipments
    log "Processed ${offset} of ${data.total}"

    offset += LIMIT
  }
  shipments.flatten()
}

def dateReturnedBelowLimit(dateReturned, topDate) {
  return parseDate(dateReturned) <= parseDate(topDate)
  //return true
}

def calculate(def ships, def topDate) {
  def shipments = JsonUtil.cleanJsonNulls(ships)

  def returnedTimes = shipments.findAll{
    it.status_history.date_returned && dateReturnedBelowLimit(it.status_history.date_returned, topDate)}.collect{
    [
      time: computeHandlingTimeService.getWorkingDays(
        parseDate(it.status_history.date_not_delivered),
        parseDate(it.status_history.date_returned),
        it.site_id
      ),
      shipment: it
    ]
  }

  def times = returnedTimes*.time as double[]
  def returned = returnedTimes*.shipment

  def avgDelayTime = new DescriptiveStatistics(times).mean

  [all: shipments, returned: returned, avgDelayTime: avgDelayTime]
}

def process = { shipments, from, to -> 
  def calc = calculate(shipments, to)
  def columns = [
    //From,To,Count,Returned,Percentage,Avg_delay_time
    "${parseDate(from).format('dd/MM/yyyy')},",
    "${parseDate(to).format('dd/MM/yyyy')},",
    "${calc.all.size()},",
    "${calc.returned.size()},",
    "${(calc.returned.size()*100/calc.all.size() as float).round(2)},",
    "${(calc.avgDelayTime as float).round(2)}\n"
  ]
  
  csv << columns.join()
}

String formatDate(Date date) {
  ISODateTimeFormat.dateTime().print(new DateTime(date.getTime()))
}

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}

Calendar parseCalendar(String date) {
  def cal = Calendar.instance
  cal.setTime(parseDate(date))
  cal
}

log "Begin process..."
try {
  [
   [from: formatDate(Date.parse('ddMMyy','090614')), to: formatDate(Date.parse('ddMMyy','270614'))],
   [from: formatDate(Date.parse('ddMMyy','300614')), to: formatDate(Date.parse('ddMMyy','180714'))]
  ].each { range ->
    
    def shipments = getShipments(range.from, range.to)
    log "shipments found: ${shipments.size()}"
    if (shipments)
      process(shipments, range.from, range.to)
  }
} catch (Exception e) {
  log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."