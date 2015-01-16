import mercadoenvios.*
import grails.converters.JSON
  
def correiosTrackingProviderService = ctx.getBean('correiosTrackingProviderService')
def correiosCarrierTrackingService = ctx.getBean('correiosCarrierTrackingService')

out = new File('/tmp/out1.log'); out.write('')
log = { text -> out << text + "\n" }
estornados = new File('/tmp/estornados.json')
resultMap = [:]
tns = new File('/tmp/tns.txt').readLines()

tns.each { tn ->
  sleep(500)
  def trackingData = correiosTrackingProviderService.getTrackingData([tn])
  def eventsForShipment = correiosCarrierTrackingService.findInResponse(trackingData, tn)
  
  log "------------------ $tn -----------------"
  if (eventsForShipment) {
      def s = Shipment.findByTrackingNumber(tn)
      def shipMap = [tracking_number: tn, service_id: s.serviceId]
      log "${s.status} - ${s.substatus}"
      log "R:${s.dateReadyToShip} S:${s.dateShipped} D:${s.dateDelivered} N:${s.dateNotDelivered}"
      def events = eventsForShipment.collect{correiosCarrierTrackingService.format(it, shipMap)}
      events.each { log "${[it.description,it.code,it.date]}\n" }
      
      resultMap[tn] = events.collect{[description: it.description, code: it.code, date: it.date]}
  } else {
      log "Not found!"
  }
  log "------------------ ------------- -----------------"
}
try {
estornados.write((resultMap as JSON).toString())
} catch (Exception e) {
	log "Exception: ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}