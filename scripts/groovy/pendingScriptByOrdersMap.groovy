import mercadoenvios.* 
import mercadoenvios.constants.ShippingConstants

out = new File('/tmp/ordersFix/ordersLog.txt'); out.write("")
ordersService = ctx.getBean("ordersService")
feedOrder = ctx.getBean("messageProcessorOrdersService")

def orderAndSender = [
	'790276897':43193744L
]

synchronized void logFile(String text) {
	out << text + "\n"
}

def processOrderForShipmentId(orderAndSender) {
	Shipment ship

	try{
		orderAndSender.each { orderId, senderId ->

			Shipment.withTransaction { tx -> 
				ship = Shipment.findByOrderId(orderId)
		    	def order = ordersService.getOrdersData(orderId, senderId)

		        if(order){
		            logFile("order_id= ${order.id} - order_status= ${order.status}")
		             
		            if (order.status == ShippingConstants.ORDERS_PAID_STATUS) {
						if (feedOrder.processOrder(order, ship)) {
							logFile("Se proceso la orderId ${order.id}")
						} else {
							tx.setRollbackOnly()
							logFile("No se pudo procesar la orderId ${order.id}")
						}
					} else {
		            	logFile("Se descarto la orderId ${order.id}")
		            }
		        } else {
		            logFile("no existe la orderId")
		        }
	        }
	    }
    } catch (Exception e){
    	logFile("Exception processing shipment ${ship?.id}: ${e}")
	}
}

processOrderForShipmentId(orderAndSender)