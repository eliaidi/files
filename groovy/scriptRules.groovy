import mercadoenvios.*
import grails.converters.JSON

def queryStr = '''select rm.shipping_from_id, rm.shipping_to_id
from shipping_location_migration lm inner join rule_migration rm on (lm.id = rm.shipping_from_id or lm.id = rm.shipping_to_id)
where lm.id not in (select l.id from shipping_location l)
group by rm.shipping_from_id, rm.shipping_to_id
having count(*) = 16'''

def calculateWeight(String expression) {
    float min = expression.split("&&")[0].split(">")[1].trim() as float
    float max = expression.split("&&")[1].split("<=")[1].trim() as float
    return (max + min) / 2
}

def result = []
ShippingLocation.withSession { session ->
  
    def query = session.createSQLQuery(queryStr) 
    
    query.list().each { 
  
        def newShipFrom = ShippingLocation.get(it[0])
        def newShipTo = ShippingLocation.get(it[1])
        def newRules = Rules.findAllByShippingFromAndShippingTo(newShipFrom, newShipTo)

        newRules.each { newRule ->
            def cityF = newShipFrom.city
            def stateF = newShipFrom.state
            def countryF = newShipFrom.country
          
            def cityT = newShipTo.city
            def stateT = newShipTo.state
            def countryT = newShipTo.country
          
            def methodId = newRule.shippingMethod.id
            def price = newRule.price
            def ruleId = newRule.id
            def serviceIds = ShippingService.findAllByShippingMethodId(methodId)*.id
            def speed = newRule.speed
            def weight = calculateWeight(newRule.expression)

            def r = [
                    'from': ['city': cityF, 'state': stateF, 'country': countryF], 
                    'method_id': methodId, 'price': price, 'rule_id': ruleId, 'service_ids': serviceIds, 'speed': speed, 'weight': weight,
                    'to': ['city': cityT, 'state': stateT, 'country': countryT]
                    ]
            
            result << r
        }
    }
}

def json = result as JSON