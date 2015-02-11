import mercadoenvios.*
import grails.converters.JSON
import grails.util.GrailsUtil

// ************************************************ Variables y beans *******************************************
out = new File('/tmp/out.log'); out.write('') //Archivo de log
log = { text -> out << text + "\n" }
notd = new File('/tmp/notd.csv')
trackingStatusService = ctx.getBean('trackingStatusService')
restClient = ctx.getBean('restClient')
// ---------------------------------------------------------------------------------------------------------------

// ************************************************ Métodos *******************************************
def processShipments() {
	notd.readLines().each { ln ->
		try {
			def tn = ln.split(",")[2]
			def shipment = Shipment.findByTrackingNumber(tn)
			def finalEvent = getFinalEvent("not_delivered", tn, shipment.serviceId)
			def substatus = trackingStatusService.getTrackingStatus(finalEvent.trackingStatus, shipment.serviceId).substatus
			if (putNotDelivered(shipment.id, substatus)) {
				log "put successful for shipment $ln - $substatus"		
			} else {
				log "put failed for shipment $ln"
			}
		} catch (Exception e) {
			log "Exception: ${e.getMessage()} - ${e.getStackTrace()}"
		}
	}
}

def getFinalEvent(event, trackingNumber, serviceId) {
	return TrackingEvents.findAllByTrackingNumber(trackingNumber).find{ 
		trackingStatusService.getTrackingStatus(it.trackingStatus, serviceId).normalizedStatus == event
	}
}

def putNotDelivered(shipmentId, substatus) {

	def result
	String uri = "/shipments/${shipmentId}?caller.scopes=admin"
	
	def body = substatus ? [status: "not_delivered", substatus: substatus] : [status: "not_delivered"]
	log "PUT a ${shipmentId}: ${body}"
	
	def jsonData = (body as JSON).toString()

	restClient.put(uri: uri,
		data: jsonData,
		success:{
			result = it.data
		},
		failure: {
			if(it.exception) {
				log "Error llamando a ${uri} - StackTrace: ${GrailsUtil.sanitize(it.exception)}"
			} else {
				int statusCode = it?.status?.getStatusCode()
				log "Server returned error code: ${it.status} ${uri}"
			}
		}
	)

	return result
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