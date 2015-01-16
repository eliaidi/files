import mercadoenvios.*

out = new File('/tmp/shipped.log')
log = { text -> out << text + '\n' }
  
def mongo = ctx.getBean('mongoService')

log "Getting shipments..."
def shippedShipments = Shipment.withCriteria{
  between 'dateCreated', new Date()-45, new Date()-5
  eq 'status', 'shipped'
  eq 'siteId', 'MLB'
  eq 'shippingMode', 'me2'
}

log "Shipments size: ${shippedShipments.size()}"

log "Finding sent shipments..."
def sentShipments = shippedShipments.findAll{ ship ->
  mongo.find('novaduqueShipmentLocks', [shipping_id: ship.id])
}

log "Sent size: ${sentShipments.size()}"

log "Shipments:"
sentShipments.each { s ->
  log "id: ${s.id} tn: ${s.tn}"
}

log "done."