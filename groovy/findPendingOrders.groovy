import mercadoenvios.*
import mercadoenvios.constants.ShippingConstants
  
def ordersService = ctx.getBean('ordersService')
//def out = new File('/tmp/orders/orders1.csv')

def shipments = Shipment.withCriteria() {
          isNotNull 'orderId'
          eq 'shippingMode', 'me2'
          eq 'status', 'pending'
          between 'dateCreated', new Date()-30, new Date()
}

shipments.each {
  
  def order = ordersService.getOrdersData(it.orderId, it.senderId)
  
  if ()
  println "order: id=${order.id} status=${order.status}"

}