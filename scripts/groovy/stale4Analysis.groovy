import mercadoenvios.Shipment
import mercadoenvios.utils.DateUtil
import mercadoenvios.TrackingEvents
import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH
import mercadoenvios.utils.JsonUtil
import static groovyx.gpars.GParsPool.withPool

ordersService = ctx.getBean('ordersService')
trackingStatusService = ctx.getBean('trackingStatusService')
restClient = ctx.getBean('restClient')
out = new File('/tmp/out.log'); out.write('')
log = { text -> out << text + '\n' }

def getShipment(shipmentId) {
  def data = null
  restClient.get(
    uri: String.valueOf("/shipments/${shipmentId.encodeAsURL()}?caller.scopes=admin"),
    success: { data = it.data },
    failure: { }
  )
  data
}

daysToAdd = [
  0: [ 0, 5, 10 ],
  1: [ 0, 5, 10 ],
  2: [ 0 ]
]

def getFinalEvent(event, trackingNumber, serviceId) {
  TrackingEvents.findAllByTrackingNumber(trackingNumber).find{ 
    trackingStatusService.getTrackingStatus(it.trackingStatus, serviceId as long).normalizedStatus == event
  }
}

def percentage(value, total) {
  return Math.round(value*100/total)
}

log "begin..."

try {
  ["stale4-19Ene_25Ene.csv","stale4-26Ene_01Feb.csv","stale4-02Feb_08Feb.csv"].eachWithIndex { fileName, index ->
    log "$fileName"
    def shipments = new File("/tmp/stale4/${fileName}").readLines().collect{ shipmentId ->
      getShipment(shipmentId)
    }

    shipments.groupBy{it.site_id}.each { siteId, shipsBySite ->
      log "SITE: $siteId"
      log "Total: ${shipsBySite.size()}"
      
      //Para cada archivo, agrego X días a los stales
      daysToAdd[index].each { toAdd ->    

        def total = shipsBySite.findAll{ ship ->
          def finalEv = ["delivered","not_delivered"].collect{ getFinalEvent(it,ship.tracking_number,ship.service_id) }.findAll{it}.sort{it.dateCreated}
          finalEv = finalEv ? finalEv.first() : null
          !finalEv ||
          ( 
            DateUtil.parseISODate(ship.shipping_option.estimated_delivery.date) +
            CH.config.stale.shipped.services[ship.service_id as long] +
            toAdd <=
            finalEv.dateCreated
          )
        }
        log "$toAdd días extra: ${total.size()}"
        log "missing: ${(shipsBySite-total)*.id}"
        
        def before = Collections.synchronizedList(new ArrayList<String>())
        def afterFinalDelivered = Collections.synchronizedList(new ArrayList<String>())
        def afterFinalNotDelivered = Collections.synchronizedList(new ArrayList<String>())
        def afterNotFinal = Collections.synchronizedList(new ArrayList<String>())
        withPool() {
          total.eachParallel { s ->
            def order = JsonUtil.cleanJsonNulls(ordersService.getOrdersData(s.order_id, s.sender_id))
            if (order.feedback.purchase?.date_created && DateUtil.parseISODate(order.feedback.purchase.date_created) <
                DateUtil.parseISODate(s.shipping_option.estimated_delivery.date))
              before << s.id
            else {
              if (getFinalEvent("delivered",s.tracking_number,s.service_id))
                afterFinalDelivered << s.id
              else if (getFinalEvent("not_delivered",s.tracking_number,s.service_id))
                  afterFinalNotDelivered << s.id
                else
                  afterNotFinal << s.id
            }
          }
        }
        def bSize = before.size()
        def aFDeliSize = afterFinalDelivered.size(); def aFNotDeliSize = afterFinalNotDelivered.size(); def aFSize = aFDeliSize + aFNotDeliSize
        def aNFSize = afterNotFinal.size()
        def aSize = aFSize + aNFSize
        if (total.size()) {
          log "before: ${bSize} - ${percentage(bSize, total.size())}%"
          log "after: ${aSize} - ${percentage(aSize, total.size())}%"
          log "- final: ${aFSize} - ${percentage(aFSize, total.size())}%"
          log "  - delivered: ${aFDeliSize} - ${percentage(aFDeliSize, total.size())}%"
          log "  - not_delivered: ${aFNotDeliSize} - ${percentage(aFNotDeliSize, total.size())}%"
          log "- notFinal: ${aNFSize} - ${percentage(aNFSize, total.size())}%"
          log ""
        }
        if (bSize)
          log "samples: before: ${before[0..(bSize <= 10 ? (bSize-1) : 10)]}"
        if (aFDeliSize)
          log "after - final - delivered: ${afterFinalDelivered[0..(aFDeliSize <= 10 ? (aFDeliSize-1) : 10)]}"
        if (aFNotDeliSize)
          log "after - final - not_delivered: ${afterFinalNotDelivered[0..(aFNotDeliSize <= 10 ? (aFNotDeliSize-1) : 10)]}"
        if (aNFSize)
          log "after - notFinal: ${afterNotFinal[0..(aNFSize <= 10 ? (aNFSize-1) : 10)]}"
      }
    }
  }
} catch (Exception e) {
      log "Exception: ${e.getMessage()} -  ${e.getStackTrace()}"
}

log "end."