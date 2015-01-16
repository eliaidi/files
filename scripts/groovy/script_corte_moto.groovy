import mercadoenvios.*

def getDia(dia) {
    return dia == 0 ? "mismo dia:" : "dia siguiente:"
}
  
def shipments = Shipment.withCriteria {
gt 'dateCreated', new Date() - 40
eq 'status', 'delivered'
eq 'siteId', 'MLA'
eq 'shippingMode', 'me2'
shippingService {
      eq 'id', 81L
}
    isNotNull 'dateDelivered'
    isNotNull 'dateShipped'
}

def shipmentsBelow11 = [:]
def shipmentsBetween11and15 = [:]
def shipmentsBetween15and20 = [:]
def shipmentsAbove20 = [:]

def groupedShipments = shipments.groupBy{it.dateDelivered - it.dateShipped}

groupedShipments.findAll{it.key in [0,1]}.each { diff, ships ->
  
    shipmentsBelow11[(diff)] = ships.findAll{(it.dateShipped.format('kk') as int) < 11}
    shipmentsBetween11and15[(diff)] = ships.findAll{(it.dateShipped.format('kk') as int) >= 11 && (it.dateShipped.format('kk') as int) < 15}
    shipmentsBetween15and20[(diff)] = ships.findAll{(it.dateShipped.format('kk') as int) >= 15 && (it.dateShipped.format('kk') as int) < 20}
    shipmentsAbove20[(diff)] = ships.findAll{(it.dateShipped.format('kk') as int) >= 20}
}

def totalshipmentsBelow11 = shipmentsBelow11.collect{it.value}.flatten().size()
def totalshipmentsBetween11and15 = shipmentsBetween11and15.collect{it.value}.flatten().size()
def totalshipmentsBetween15and20 = shipmentsBetween15and20.collect{it.value}.flatten().size()
def totalshipmentsAbove20 = shipmentsAbove20.collect{it.value}.flatten().size()
def total = totalshipmentsBelow11 + totalshipmentsBetween11and15 + totalshipmentsAbove20 + totalshipmentsBetween15and20

println "-------------------------------------"
println "totales:"
println "< 11hs:" + totalshipmentsBelow11 + " - " + (totalshipmentsBelow11*100/total) + " %"
println ">=11hs <= 15hs:" + totalshipmentsBetween11and15 + " - " + (totalshipmentsBetween11and15*100/total) + " %"
println ">=15hs <= 20hs:" + totalshipmentsBetween15and20 + " - " + (totalshipmentsBetween15and20*100/total) + " %"
println "> 20hs:" + totalshipmentsAbove20 + " - " + (totalshipmentsAbove20*100/total) + " %"
println "total: " + total

println "-------------------------------------"
println "< 11hs"
shipmentsBelow11.each{ diff, ships ->
  	println "${getDia(diff)} ${ships.size()*100/totalshipmentsBelow11} %"
}

println "-------------------------------------"
println ">=11hs <= 15hs"
shipmentsBetween11and15.each{ diff, ships ->
	println "${getDia(diff)} ${ships.size()*100/totalshipmentsBetween11and15} %"
	if (diff == 1) {
		def withFirstVisit = shipmentsBetween11and15[1].findAll{it.dateFirstVisit < it.dateDelivered}.size()
		def withOutFirstVisit = shipmentsBetween11and15[1].findAll{it.dateFirstVisit == it.dateDelivered}.size()
		println "	con primera visita: ${withFirstVisit*100/(withFirstVisit+withOutFirstVisit)}"
		println "	sin primera visita: ${withOutFirstVisit*100/(withFirstVisit+withOutFirstVisit)}"
	}
}

println "-------------------------------------"
println ">=15hs <= 20hs"
shipmentsBetween15and20.each{ diff, ships ->
	println "${getDia(diff)} ${ships.size()*100/totalshipmentsBetween15and20} %"
}

println "-------------------------------------"
println "> 20hs"
shipmentsAbove20.each{ diff, ships ->
	println "${getDia(diff)} ${ships.size()*100/totalshipmentsAbove20} %"
}
println "-------------------------------------"

println "done"