import mercadoenvios.*
import groovy.util.XmlSlurper

out = new File('/tmp/novaduque.log')
log = { text -> out << text + "\n" }

def novaduqueService = ctx.getBean('novaduqueService')
/*
def shipmentsSent = [21005359675]  

log "Getting shipments..."
def shipments = Shipment.withCriteria {
    eq 'siteId', 'MLB'
    ge 'dateCreated', new Date()-1
    'in' 'status', 'shipped'
}
log "found: ${shipments.size()}"

def shipmentsToSend = [shipments.find{!shipmentsSent.contains(it.id)}]
log "sending shipments ${shipmentsToSend*.id}"

def result = novaduqueService.postShipments(shipmentsToSend)
log "result: $result"
*/
/*
def response = novaduqueService.getShipmentData('SW814893384BR')
println "s: " + response?.status
println "d: " + response?.data
*/

novaduqueService = ctx.getBean('novaduqueService')
def response = novaduqueService.getShipmentData('SW814893384BR')

println "r: ${response.dump()}"


/////////////////////////////////////////////

import mercadoenvios.*

import groovy.util.XmlSlurper

out = new File('/tmp/novaduque.log')
log = { text -> out << text + "\n" }

def novaduqueService = ctx.getBean('novaduqueService')

def shipmentsSent = [21005359675]  

log "Getting shipments..."
def shipments = Shipment.get(21005498261)
/*{
    eq 'siteId', 'MLB'
    ge 'dateCreated', new Date()-1
    'in' 'status', 'shipped'
}
log "found: ${shipments.size()}"

def shipmentsToSend = [shipments.find{!shipmentsSent.contains(it.id)}]
log "sending shipments ${shipmentsToSend*.id}"
def result = novaduqueService.postShipments(shipmentsToSend)
*/

def result = novaduqueService.postShipments([shipments])
log "result: $result"

/*
restConnector = ctx.getBean('restConnector')
def response = restConnector.execGet('http://meli.novaduque.com.br/envio/xml/970cc5c0-641e-11e3-949a-0800200c9a66/SW814893384BR','XML')

println "r: $response"
println response.data.getClass()

def objeto = new XmlSlurper().parseText(response.data)

println objeto.size()
println objeto.endereco.text() && objeto.numero.text() ? "si h" : "no h"
println objeto.data_envio
println objeto.endereco
println objeto.numero.text().getClass()
println objeto.registro.getClass()
println objeto.v_postal.text()
println "complemento_r: ${objeto.complemento_r.text()?'si':'no'}"

def bd = new BigDecimal(objeto.v_postal.text())
println bd
*/