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
out = new File('/tmp/out.log'); out.write('')
log = { text -> out << text + "\n" }
getTime = { init , end -> def time; use(groovy.time.TimeCategory) { time = end - init } }

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
                                    "shipment.site_id": "MLB"
                                }
                            },
                            {
                                "term": {
                                    "shipment.mode": "me2"
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
                                "query_string": {
                                    "default_field": "shipment.status",
                                    "query": "delivered OR not_delivered OR shipped"
                                }
                            },
                            {
                                "range": {
                                    "shipment.status_history.date_shipped": {
                                        "from": "'''+from+'''",
                                        "to": "'''+to+'''"
                                    }
                                }
                            }
                        ],
                        "must_not": [
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
                        "should": []
                    }
                },
                  "from": '''+offset+''',
                  "size": '''+limit+''',
                  "sort": [],
                  "facets": {}
                }'''
}

def logFinalStats = { from, to, finalResult ->
  log "STATS from $from to $to"
  if (finalResult.diffs.delivered) {
    log "--- delivered ---"
    log "Days: ${((new DescriptiveStatistics(finalResult.diffs.delivered as double[]).mean) as float).round(2)}"
    log "-----------------"
  }
  if (finalResult.diffs.delivered) {
    log "--- not_delivered ---"
    log "Days: ${((new DescriptiveStatistics(finalResult.diffs.not_delivered as double[]).mean) as float).round(2)}"
    log "---------------------"
  }
  if (finalResult.stales) {
    log "--- stales ---"
    log "total: ${finalResult.total}"
    log "stales: ${finalResult.stales} - ${((finalResult.stales*100/finalResult.total) as float).round(2)} %"
    log "--------------"
  }
}

def stats = { results, finalResult ->
  if (results.diffs.delivered)
    finalResult.diffs.delivered << new DescriptiveStatistics(results.diffs.delivered as double[]).mean

  if (results.diffs.not_delivered)
    finalResult.diffs.not_delivered << new DescriptiveStatistics(results.diffs.not_delivered as double[]).mean
    
  if (results.stales)
    finalResult.stales += results.stales

  finalResult.total += results.total
}

def process = { shipments, finalResult ->
  def results = [diffs: [delivered: [], not_delivered:[]], stales: 0, total: shipments.size()]
  JsonUtil.cleanJsonNulls(shipments).each { ship ->

    if (ship.status_history.date_delivered) {
      results.diffs.delivered << getTime( parseDate(ship.status_history.date_shipped), parseDate(ship.status_history.date_delivered) ).days

      if ( parseDate(ship.status_history.date_shipped) < parseDate(ship.status_history.date_delivered)-21 )
        results.stales++
    
    } else if (ship.status_history.date_not_delivered) {
      results.diffs.not_delivered << getTime( parseDate(ship.status_history.date_shipped), parseDate(ship.status_history.date_not_delivered) ).days

      if ( parseDate(ship.status_history.date_shipped) < parseDate(ship.status_history.date_not_delivered)-21 )
        results.stales++
    
    } else if ( parseDate(ship.status_history.date_shipped) < new Date()-21 ) {
      results.stales++
    }

  }

  return stats(results, finalResult)
}

def processShipments = { from, to ->
  log "Processing shipments from $from to $to"
  finalResult = [diffs: [delivered: [], not_delivered:[]], stales: 0, total: 0]
  int offset = 0
  int LIMIT = 10000

  def data = getData(offset, LIMIT, from, to)
  log "Total shipments to process: ${data.total}"
  offset += LIMIT

  process(data.shipments, finalResult)
  while (offset < data.total) {
    
    process(getData(offset, LIMIT, from, to).shipments, finalResult)
    log "Processed ${offset} of ${data.total}"
    offset += LIMIT
  }

  logFinalStats(from, to, finalResult)
}

String formatDate(Date date) {
  ISODateTimeFormat.dateTime().print(new DateTime(date.getTime()))
}

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}

log "Begin process..."

processShipments("2014-02-28T00:00:00.000-04:00","2014-03-28T00:00:00.000-04:00")
processShipments("2014-06-28T00:00:00.000-04:00","2014-07-28T00:00:00.000-04:00")

log "End."