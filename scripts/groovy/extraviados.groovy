import mercadoenvios.*
import static mercadoenvios.constants.ShippingConstants.*

def out = new File('/tmp/out.log')
def trackings = new File('/tmp/trackings.txt')
def results = new File('/tmp/results.log')
def ordersService = ctx.getBean('ordersService')
def paymentsService = ctx.getBean('paymentsService')

log = { text -> out << text + "\n" }

log "processing tns"
def total = 0
trackings.eachLine { tn ->
    if (((total++)%1000)==0) {
        log "processing $total of 8647"
        sleep(1000)
    }
    def ordersData
    try {
        def shipment = Shipment.findByTrackingNumber(tn)
        if (shipment) {
          def orderId = shipment.orderId
          def callerId = shipment.receiverId
          ordersData = ordersService.getOrdersData(orderId, callerId)
          def paymentIds = ordersData.payments*.id
  
          if (paymentIds) {
              def payments = paymentIds.collect{paymentsService.getPaymentData(it, callerId)}.findAll{it}
              if (payments) {
                  def payment = payments.find{it.operation_type == REGULAR_PAYMENT_OPERATION_TYPE}
                  results << "${ordersData?.id} - ${payment.status}" + "\n"
              }
          }
        } else
          log "Shipment ${tn} not found"
        
    } catch (Exception e){
        results << "${ordersData?.id}: exception. ${e}" + "\n"
    }
}
log "done"