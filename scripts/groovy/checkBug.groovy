import mercadoenvios.*
import grails.converters.JSON
import grails.util.GrailsUtil
import static groovyx.gpars.GParsPool.withPool

// ************************************************ Variables y beans *******************************************
out = new File('/tmp/out.log'); out.write('') //Archivo de log
falseFb = new File('/tmp/falseFb.csv')
log = { text -> out << text + "\n" }
deli = new File('/tmp/deli.csv'); deli.write(''); 
synchronized void addDelivered(text) { deli << text + "\n" }
ship = new File('/tmp/ship.csv'); ship.write('');
synchronized void addShipped(text) { ship << text + "\n" }
notd = new File('/tmp/notd.csv'); notd.write('');
synchronized void addNotDelivered(text) { notd << text + "\n" }
ordersService = ctx.getBean('ordersService')
trackingStatusService = ctx.getBean('trackingStatusService')
restClient = ctx.getBean('restClient')

dataFetchers = [
	'MLA': ctx.getBean('ocaTrackingDataFetcher'),
	'MLB': ctx.getBean('correiosDS2TrackingDataFetcher'),
	'MLM': ctx.getBean('dhlTrackingDataFetcher')
]

// ---------------------------------------------------------------------------------------------------------------

// ************************************************ Métodos *******************************************
def processShipments() {
    def lines = falseFb.readLines().unique()
    log "unique lines ${lines.size()}"
	withPool(4) {
			lines.eachParallel { ln ->
			try {
				def tn = ln.split(",")[2]
				def shipment = Shipment.findByTrackingNumber(tn)
				def finalEvent = getDeliveredEvent(shipment)
				if (finalEvent) {
					addDelivered(ln)
				} else {
					finalEvent = getNotDeliveredEvent(tn, shipment.serviceId)
					if (finalEvent) {
						def substatus = trackingStatusService.getTrackingStatus(finalEvent.trackingStatus, shipment.serviceId).substatus
						addNotDelivered(substatus ? "$ln,$substatus" : "$ln,no_substatus")
						
					} else {
						addShipped(ln)
					}
				}
			} catch (Exception e) {
				log "Exception: ${e.getMessage()} - ${e.getStackTrace()}"
			}
		}
	}
}

def getNotDeliveredEvent(trackingNumber, serviceId) {
	return TrackingEvents.findAllByTrackingNumber(trackingNumber).find{ 
		trackingStatusService.getTrackingStatus(it.trackingStatus, serviceId)?.normalizedStatus == "not_delivered"
	}
}

def getDeliveredEvent(shipment) {
	def dataFetcher = dataFetchers[shipment.siteId]
    def trackingData = dataFetcher.getTrackingData([[tracking_number: shipment.trackingNumber]])
    def eventsForShipment = dataFetcher.findInResponse(trackingData, [tracking_number: shipment.trackingNumber]) 
	def events = eventsForShipment?.collect { dataFetcher.carrierTrackingService.format(it, [id: shipment.id, service_id: shipment.serviceId, tracking_number: shipment.trackingNumber]) }
	events.find { trackingStatusService.getTrackingStatus(it.code, it.service_id)?.normalizedStatus == "delivered" }
}

// ---------------------------------------------------------------------------------------------------------------


// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
	processShipments()
	
} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------