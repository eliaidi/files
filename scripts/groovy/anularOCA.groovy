import mercadoenvios.*
import mercadoenvios.oca.AnularOrdenGeneradaResponse.AnularOrdenGeneradaResult

  
/*
def shipment
Shipment.withTransaction{
   shipment = Shipment.get(21379396063)
   shipment.status = 'handling'
   shipment.save(failOnError:true)
}
  

authorizationDataFetcher = ctx.getBean('authorizationDataFetcher')

println "shipment status: ${shipment.status}"
def msg = authorizationDataFetcher.format(shipment)

ocaAuthorizationProcessorService = ctx.getBean('ocaAuthorizationProcessorService')

if (ocaAuthorizationProcessorService.validateMessage(msg)) {
    def result = ocaAuthorizationProcessorService.processMessage(msg)
    println "result: $result"
} else
  println "noo"
*/
  
//ocaEPakService = ctx.getBean('ocaEPakService')
//def result = ocaEPakService.cancelTrackingRequest((Shipment.get(21379396063).orderId) as long)

def u = "mercadoenvios-ar@mercadolibre.com"
def p = "MER19TES"
int o = Shipment.get(21379396063).orderId as int
ocaSoapClient = ctx.getBean('ocaSoapClient')

println "u: $u - p: $p - o: $o"
def result = ocaSoapClient.anularOrdenGenerada(u,p,o)
  
println "result: $result"