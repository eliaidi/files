import mercadoenvios.*

out = new File('/tmp/tn.log')
log = { text -> out << text + '\n' }

log "Processing file..."
def file = new File('/tmp/tn.txt')

file.eachLine { ln ->
	String tn = String.valueOf(ln).trim()
    def shipment = Shipment.findByTrackingNumber(tn)
    
    if (shipment) {
      if (shipment.dateDelivered || shipment.dateNotDelivered) {
          if (shipment.status == 'shipped') {
              log "Processing shipment ${shipment.id}"
              shipment.dateDelivered = null
              shipment.dateNotDelivered = null
              shipment.save()
          }
      }
    } else {
        log "Shipment with tn: $tn not found"
    }
}