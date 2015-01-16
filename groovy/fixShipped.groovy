import mercadoenvios.*
import org.hibernate.FetchMode as FM
import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH
  
out = new File('/tmp/shipped.log')
log = { text -> out << text + '\n' }
  
log "Getting shipments..."
def file = new File('/tmp/shipments.txt')

def meetConditions(shipment) {
  def correiosEvent = shipment.trackings.find{it.statusId in ['BDR00','BDI01','BDE01','BDE00','BDR01','BDI00']}
  def ndEvent = shipment.trackings.find{it.applicationId in CH.config.tracking.applications.novaduque}
  if (correiosEvent && ndEvent && shipment.status = 'shipped')
    return correiosEvent
  else
    return null
}

file.eachLine{ shipId ->
   
  def shipment = Shipment.createCriteria().get {
      eq 'id', Long.valueOf(shipId)
      fetchMode 'trackings', FM.JOIN
  }
  def event = meetConditions(shipment)
  if (event) {
      log "Updating shipment ${shipment.id}"
      shipment.status = 'delivered'
      shipment.dateDelivered = event.eventDate
      shipment.save()
  } else {
    log "Shipment ${shipment.id} doesn't meet conditions"
  }
}

log "done."