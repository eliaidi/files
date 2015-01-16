import mercadoenvios.*

def novaduqueService = ctx.getBean('novaduqueService')
out = new File('/tmp/novaduqueTest.log')
log = { text -> out << text + "\n" }

def sentShipmentIds = []
def last = new Date(new Date().time-15*60*1000)

log "Beginning novaduque test..."
while (true) {
  log "sentShipmentIds: $sentShipmentIds"
  log "total: ${sentShipmentIds.size()}"
  
  log "Getting shipments..."
  def shipments = Shipment.withCriteria {
    gt 'dateCreated', new Date() - 1
    between 'dateShipped', last, new Date()
    eq 'siteId', 'MLB'
    eq 'status', 'shipped'
    eq 'shippingMode', 'me2'
  }
  last = new Date()
  
  if (shipments) {
    def shipmentsToSend = shipments.findAll{!(it.id in sentShipmentIds)}
    
    if (shipmentsToSend) {
      log "Sending shipments ${shipmentsToSend.collect{[id:it.id, tn:it.trackingNumber]}}"
      novaduqueService.postShipments(shipmentsToSend)
      
      sentShipmentIds << shipmentsToSend.collect{it.id}
      sentShipmentIds = sentShipmentIds.flatten()
    } else
      log "No shipments to send."  
  } else
    log "No shipments found."
  
  log "Sleeping for 15 minutes..."
  sleep(1000*60*15)
}