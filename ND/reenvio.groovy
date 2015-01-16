import mercadoenvios.*
import static groovyx.gpars.GParsPool.withPool

mongoService = ctx.getBean('mongoService')
messageProcessor = ctx.getBean('novaduqueMessageProcessorService')

input = new File('/tmp/nd/missing.csv')

output = new File('/tmp/nd/output.log'); output.write('')
synchronized void log(text) { output << text + "\n" }

conf = new File('/tmp/nd/confirm.log'); conf.write('')
synchronized void confirm(text) { conf << text + "\n" }

ret = new File('/tmp/nd/retry.log'); ret.write('')
synchronized void retry(text) { ret << text + "\n" }

fail = new File('/tmp/nd/failed.log'); fail.write('') 
synchronized void failed(text) { fail << text + "\n" }

def doProcess(message) {
  try {
    if (messageProcessor.processMessage(message)) {
      confirm "${message.shipment_id}"
              
    } else {
      messageProcessor.processFailedMessage(message)
      retry "${message.shipment_id}"
    }
  } catch (Exception e) {
    messageProcessor.processFailedMessage(message)
    failed "${message.shipment_id}: ${e.getMessage()}"
  }
}

log "Begin process..."
withPool(4) {
    input.readLines().eachParallel { ln ->
  
    def tn = ln.split(",").first()
    if (tn.size() == 13) {
      def shipment = Shipment.findByTrackingNumber(tn)
      if (shipment) {
        def msg = [
                    shipment_id: shipment.id,
                    service_id: shipment.serviceId,
                    mode: shipment.shippingMode
        ]
        doProcess(msg)
        log "Processed message $msg"
      } else {
        log "Shipment not found for tn: $tn"
      }
    } else {
      log "Invalid tn: $tn"
    }
  }
}
log "End."