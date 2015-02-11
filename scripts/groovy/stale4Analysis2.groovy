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
    success: { data = JsonUtil.cleanJsonNulls(it.data) },
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
    }.findAll{it}

    shipments.groupBy{it.site_id}.each { siteId, shipsBySite ->
      log "SITE: $siteId"
      log "Total: ${shipsBySite.size()}"
      
      //Para cada archivo, agrego X días a los stales
      daysToAdd[index].each { toAdd ->    

        def total = shipsBySite.findAll{ ship ->
          //Busco el evento finalizador, para obtener el dateCreated que es la fecha en la cual lo recibimos. No puedo usar date_delivered o date_not_delivered
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
        
        def delivered = Collections.synchronizedList(new ArrayList<String>())
        def notDelivered = Collections.synchronizedList(new ArrayList<String>())
        def shippedInMediation = Collections.synchronizedList(new ArrayList<String>())
        def shippedNoMediation = Collections.synchronizedList(new ArrayList<String>())
        withPool() {
          total.eachParallel { s ->
            if (s.status == 'shipped') {
              def order = JsonUtil.cleanJsonNulls(ordersService.getOrdersData(s.order_id, s.sender_id))
              if (order.mediations)
                shippedInMediation << s.id
              else
                shippedNoMediation << s.id
            } else if (s.status == 'delivered') {
              delivered << s.id
            } else if (s.status == 'not_delivered') {
              notDelivered << s.id
            }
          }
        }
        
        if (total.size()) {
          def shipped = shippedNoMediation + shippedInMediation
          log "delivered: ${delivered.size()} - ${percentage(delivered.size(), total.size())}%"
          log "not_delivered: ${notDelivered.size()} - ${percentage(notDelivered.size(), total.size())}%"
          log "shipped: ${shipped.size()} - ${percentage(shipped.size(), total.size())}%"
          log "- no mediation: ${shippedNoMediation.size()} - ${percentage(shippedNoMediation.size(), total.size())}%"
          log "- with mediation: ${shippedInMediation.size()} - ${percentage(shippedInMediation.size(), total.size())}%"
          log ""
        }
        if (delivered)
          log "sample delivered: ${delivered.first()}"
        if (notDelivered)
          log "sample notDelivered: ${notDelivered.first()}"
        if (shippedInMediation)
          log "sample shippedInMediation: ${shippedInMediation.first()}"
        if (shippedNoMediation)
          log "sample shippedNoMediation: ${shippedNoMediation.first()}"
      }
    }
  }
} catch (Exception e) {
      log "Exception: ${e.getMessage()} -  ${e.getStackTrace()}"
}

log "end."