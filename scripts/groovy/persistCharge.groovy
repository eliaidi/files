import mercadoenvios.*
  
def ordersService = ctx.getBean("ordersService")
def messageProcessorOrdersService = ctx.getBean("messageProcessorOrdersService")

def orders = [
'788114203':21794601L,
'788137374':2240331L,
'788128599':19400838L,
'786867404':44769378L
]

int generated=0;int notGenerated=0;int paid=0;int notPaid=0
println "total: ${orders.size()}"

orders.each { orderId, senderId ->

  def order = ordersService.getOrdersData(orderId, senderId)
    
  if (order) {
	def shipment = Shipment.findByOrderId(orderId)
	
    if (order.status == "paid") {
      paid++
      if (messageProcessorOrdersService.persistCharge(order, shipment)) {
        println "Charge generated for orderId ${orderId}"
        generated++
      } else {
        println "Couldnt generate charge for orderId ${orderId}"
        notGenerated--
      }
    } else {
      notPaid++
    }
  } else {
    println "$orderId not found"
  }
    
}

println "gen: $generated - notGen: $notGenerated - paid: $paid - notPaid: $notPaid"