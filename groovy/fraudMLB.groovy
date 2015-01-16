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
import mercadoenvios.utils.RetriesUtil

restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('')
log = { text -> out << text + "\n" }
ordersService = ctx.getBean('ordersService')
correiosCarrierTrackingService = ctx.getBean('correiosCarrierTrackingService')
correiosTrackingProviderService = ctx.getBean('correiosTrackingProviderService')

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
                                    "query": "pending"
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
                            },
                            {
                                "constant_score": {
                                    "filter": {
                                        "missing": {
                                            "field": "shipment.status_history.date_delivered"
                                        }
                                    }
                                }
                            },
                            {
                                "constant_score": {
                                    "filter": {
                                        "missing": {
                                            "field": "shipment.status_history.date_not_delivered"
                                        }
                                    }
                                }
                            },
                            {
                                "range": {
                                    "shipment.date_created": {
                                        "gte": "2014-05-01T00:00:00.000-04:00"
                                    }
                                }
                            }
                        ],
                        "must_not": [
                            {
                                "constant_score": {
                                    "filter": {
                                        "missing": {
                                            "field": "shipment.status_history.date_ready_to_ship"
                                        }
                                    }
                                }
                            },
                            {
                                "constant_score": {
                                    "filter": {
                                        "missing": {
                                            "field": "shipment.tracking_number"
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

results = Collections.synchronizedMap(new HashMap<String, Set<String>>())
results.fraud = Collections.synchronizedSet(new HashSet<String>())
results.ok = Collections.synchronizedSet(new HashSet<String>())
results.notEnough = Collections.synchronizedSet(new HashSet<String>())
results.notRefunded = Collections.synchronizedSet(new HashSet<String>())

def process = { shipments ->
  
  GParsPool.withPool(7) {
      JsonUtil.cleanJsonNulls(shipments).eachParallel { ship ->
      def order = ordersService.getOrdersData(ship.order_id, ship.sender_id)
      def refundedPayments = order?.payments.findAll{it.status == "refunded"}
      
      if (refundedPayments)
        if (refundedPayments.collect{it.transaction_amount}.sum() >= order.total_amount)
          if (hasEvents(ship.tracking_number))
            results.fraud << ship.id
          else
            results.ok << ship.id
        else
          results.notEnough << ship.id
      else
        results.notRefunded << ship.id
    }
  }
}

boolean hasEvents(def trackingNumber) {
  try {
    def trackingData = RetriesUtil.withRetries(3, 300, "Getting tracking data") {
      correiosTrackingProviderService.getTrackingData([trackingNumber])
    }
    return correiosCarrierTrackingService.findInResponse(trackingData, trackingNumber)
    
  } catch (Exception e) {
    logR.error "Error while checking tracking status in Correios for $trackingNumber", e
  }
}

def processShipments = { 
  log "Processing shipments"
  int offset = 0
  int LIMIT = 1000

  def data = getData(offset, LIMIT)
  log "Total shipments to process: ${data.total}"
  offset += LIMIT

  process(data.shipments)
  while (offset < data.total) {
    
    process(getData(offset, LIMIT).shipments)
    log "Processed ${offset} of ${data.total}"
    offset += LIMIT
  }

  log "RESULTS"
  log "$results"
  log "${results.collect{[it.key, it.value.size()]}}"
}

log "Begin process..."

processShipments()

log "End."