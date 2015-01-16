import mercadoenvios.*
import mercadoenvios.conciliation.*
import org.hibernate.FetchMode
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import java.math.*

daysBack = 14
out = new File('/tmp/prices.log'); out.write('')
def rulesMongoService = ctx.getBean('rulesCorreiosMLBService')
rulesMongoService.collectionName = 'rulesMLBUpdated'

void log(message){
	out << message.toString() + "\n"
}
def round(value) {
	return new BigDecimal(value).setScale(2, RoundingMode.HALF_UP)
}
def percent(listValue, listTotal) {
	return round(listValue.size()*100/listTotal.size())
}

void percentages(differences) {
	def below1 = differences.findAll{it < 1}
	def below5 = differences.findAll{it >= 1 && it < 5}
	def below10 = differences.findAll{it >= 5 && it < 10}
	def below100 = differences.findAll{it >= 10 && it < 100}
	def above100 = differences.findAll{it >= 100}
    log "Percentages:"
	log "< 1: ${below1.size()} - ${percent(below1,differences)}%"
	log "< 5: ${below5.size()} - ${percent(below5,differences)}%"
	log "< 10: ${below10.size()} - ${percent(below10,differences)}%"
	log "< 100: ${below100.size()} - ${percent(below100,differences)}%"
	log "> 100: ${above100.size()} - ${percent(above100,differences)}%"
}

log "Getting shipments..."
//Busco datos envios de brasil
def data = RealShipmentData.withCriteria {
    eq 'siteId', 'MLB'
    gt 'dateCreated', new Date() - daysBack
    fetchMode 'shipment', FetchMode.JOIN
    shipment {
        'in' 'status', ['ready_to_ship','shipped','delivered','not_delivered']
        eq 'cost', BigDecimal.ZERO
    }
}

log "Calculating positives..."
//Busco envios que cobremos mas de lo nos cobra Correios
def differencesNow = []
def positives = data.findAll{it.shipment.realCost - it.amountCharged >= 0}
positives.each{
  //log "id: ${it.shipment.id} r: ${it.shipment.realCost} c: ${it.amountCharged} dif: ${it.shipment.realCost - it.amountCharged}"
  differencesNow << it.shipment.realCost - it.amountCharged
}
def differencesLater = []
positives = data.findAll{rulesMongoService.getRule(it.shipment.appliedShippingRuleId).price - it.amountCharged >= 0}
positives.each{
	differencesLater << rulesMongoService.getRule(it.shipment.appliedShippingRuleId).price - it.amountCharged
}
log "POSITIVES"
log new DescriptiveStatistics(differencesNow as double[])
percentages(differencesNow)

log new DescriptiveStatistics(differencesLater as double[])
percentages(differencesLater)


log "Calculating negatives.."
//Busco envios que cobremos menos de lo que nos cobra Correios
differencesNow = []
def negatives = data.findAll{it.shipment.realCost - it.amountCharged < 0}
negatives.each{
  //log "id: ${it.shipment.id} r: ${it.shipment.realCost} c: ${it.amountCharged} dif: ${it.shipment.realCost - it.amountCharged}"
  differencesNow << (it.shipment.realCost - it.amountCharged)*(-1)
}
differencesLater = []
negatives = data.findAll{rulesMongoService.getRule(it.shipment.appliedShippingRuleId).price - it.amountCharged < 0}
negatives.each{
    if ( (rulesMongoService.getRule(it.shipment.appliedShippingRuleId).price - it.amountCharged)*(-1) > 100 )
    	log "id: ${it.shipment.id} r: ${rulesMongoService.getRule(it.shipment.appliedShippingRuleId).price} c: ${it.amountCharged} dif: ${(rulesMongoService.getRule(it.shipment.appliedShippingRuleId).price - it.amountCharged)*(-1)}"
	differencesLater << (rulesMongoService.getRule(it.shipment.appliedShippingRuleId).price - it.amountCharged)*(-1)
}
log "\n\nNEGATIVES"
log new DescriptiveStatistics(differencesNow as double[])
percentages(differencesNow)

log new DescriptiveStatistics(differencesLater as double[])
percentages(differencesLater)
