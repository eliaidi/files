import mercadoenvios.*
import org.hibernate.FetchMode
  
def priceCalculatorService = ctx.getBean('priceCalculatorService')
def mongoService = ctx.getBean('mongoService')
  

def shipmentIds = [20707148900L,20707080016L,20707136856L,20707101062L,20707101880L,
                   20707102535L,20707108657L,20707231024L,20707165066L,20707172934L,
                   20707245442L,20707250576L,20707182608L,20707184655L,20707258195L,
                   20707258548L,20707189192L,20707262606L,20707263792L,20707268688L,
                   20707272676L,20707203276L,20707210617L,20707211711L,20707281940L,
                   20707215325L,20707285743L,20707215824L,20707216967L,20707287861L,
                   20707295235L,20707224740L,20707297801L,20707225959L,20707300909L,
                   20707229139L,20707229115L,20707230627L,20707332373L,20707308217L,
                   20707334259L,20707335032L,20707337889L,20707662328L,20707572764L,
                   20707572882L,20707573258L,20707573073L,20707663241L,20707662883L]


shipmentIds.each { id ->
  
    Shipment.withTransaction {
    
        def s = Shipment.withCriteria(uniqueResult: true) {
            eq 'id', id
            fetchMode 'receiverAddress', FetchMode.JOIN
        }
      
        def params = [item_id: s.itemId, zip_code: s.receiverAddress.zipCode]
        println "params: $params"
      
        def result = priceCalculatorService.getCalculatorData(params)
        println "result.options: ${result.options}"
      
        def ruleId = result.options.find{it.shipping_method_id == s.shippingMethodId}?.id
        if (ruleId) {
          //TODO modificar coleccion rulesMLB o rulesMLA en base al siteId del shipment
          if (mongoService.findOne('rulesMLB',[rule_id: ruleId])) {
              println "To save shipment ${s.id} with rule ${ruleId}"
              s.appliedShippingRuleId = ruleId as Long
              s.save()
          } else {
              println "Rule ${ruleId} not found in mongo"
          }
        } else {
            println "ruleId $ruleId not found in calculator"
        }
    }
}

