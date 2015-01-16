import grails.converters.JSON
import org.apache.http.conn.params.ConnRoutePNames
import mercadoenvios.*
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime
import static groovyx.gpars.GParsPool.withPool
import java.util.concurrent.atomic.AtomicInteger
import org.hibernate.FetchMode as FM
import static mercadoenvios.constants.ShippingConstants.*

// ************************************************ Variables y beans *******************************************
restConn = ctx.getBean('restConnector')
restConn.servClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, null)
out = new File('/tmp/out.log'); out.write('') //Archivo de log
synchronized void log(text) { out << text + "\n" }
trackingStatusService = ctx.getBean('trackingStatusService')

shipmentIds = []//TODO
// ---------------------------------------------------------------------------------------------------------------


def process(shipId) {
	
    def shipment
    try {
        Shipment.withTransaction {
            shipment = Shipment.createCriteria().get {
                eq 'id', shipId
                fetchMode 'trackings', FM.JOIN
            }
            if (shipment) {
                def shippedEvents = shipment.trackings?.findAll{
                    trackingStatusService.getTrackingStatus(it.trackingStatus, shipment.serviceId)?.normalizedStatus == SHIP_STATUS_SHIPPED
                }
                def shippedEvent = null
                if (shippedEvents)
                    shippedEvent = shippedEvents.sort{it.eventDate}.first()

                if (shippedEvent) {
                    shipment.dateShipped = shippedEvent.eventDate
                    shipment.save(failOnError: true)
                    log "id: ${shipment.id} tn: ${shipment?.trackingNumber} -> dateShipped: ${shippedEvent?.eventDate}"	
                }
            }
        }
        
    } catch (Exception e) {
        log "Exception ${shipment?.id}: - ${e.getMessage()} - ${e.getStackTrace()}"
    }
    
}


// ************************************************ Proceso principal *******************************************
log "Begin process..."
try {
    shipmentIds.each{
      process(it)
    }

} catch (Exception e) {
	log "Exception - ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}
log "End process."
// ---------------------------------------------------------------------------------------------------------------