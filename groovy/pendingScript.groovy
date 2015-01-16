import mercadoenvios.* 
import mercadoenvios.constants.ShippingConstants
import groovyx.gpars.GParsPool

out = new File('/tmp/ordersFix/ordersLog.txt'); out.write("")
ordersService = ctx.getBean("ordersService")
feedOrder = ctx.getBean("messageProcessorOrdersService")
DAYS_BEFORE = 3


synchronized void logFile(String text) {
	out << text + "\n"
}

def processOrderForShipmentId(shipmentId) {
	Shipment ship

	try{
		Shipment.withTransaction { tx -> 
			ship = Shipment.get(shipmentId)
	    	def order = ordersService.getOrdersData(ship.orderId, ship.senderId)

	        if(order){
	            logFile("order_id= ${order.id} - order_status= ${order.status}")
	             
	            if (order.status == ShippingConstants.ORDERS_PAID_STATUS) {
					/*if (feedOrder.processOrder(order, ship)) {
						logFile("Se proceso la orderId ${order.id}")
					} else {
						tx.setRollbackOnly()
						logFile("No se pudo procesar la orderId ${order.id}")
					}*/
					logFile("Se debe procesar la orderId ${order.id}")
	            } else {
	            	logFile("Se descarto la orderId ${order.id}")
	            }
	        } else {
	            logFile("no existe la orderId")
	        }
        }
    } catch (Exception e){
    	logFile("Exception processing shipment ${ship?.id}: ${e}")
	}
}

def processScriptForSite(siteId) {
	
	try {

		out << "Getting pending shipments...\n"

		def shipments = Shipment.withCriteria() {
	          isNotNull 'orderId'
	          eq 'shippingMode', 'me2'
	          eq 'status', 'pending'
	          eq 'siteId', siteId
	          between 'dateCreated', new Date() - DAYS_BEFORE, new Date()

	          projections {
	          	property 'id'
	          }
		}

		def total = shipments.size()
		out << "Total shipments found: ${total}\n"

		GParsPool.withPool(7) { p ->
			shipments.eachWithIndexParallel { id, index ->
		    	
				logFile("Processing shipment ${index+1} of ${total} with id ${id}")
		    	
		    	processOrderForShipmentId(id)
		        
		    	logFile("End processing shipment ${index+1} of ${total} with id ${id}")
		  	}
	  	}
	} catch (Exception e) {
		out << "Exception: ${e}\n"
	}
}

def processScript() {

	out << "Processing site MLA...\n"
	processScriptForSite('MLA')
	out << "End processing site MLA\n"

	
	out << "Processing site MLB...\n"
	processScriptForSite('MLB')
	out << "End processing site MLB\n"

}

processScript()
