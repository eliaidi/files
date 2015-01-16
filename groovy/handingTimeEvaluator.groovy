import mercadoenvios.*
  
def redisHandlingTimeService = ctx.getBean('redisHandlingTimeService')
def computeHandlingTimeService = ctx.getBean('computeHandlingTimeService')
out = new File('/tmp/handlingTimeLog.log')
def DAYS_BEFORE = 15

def log(String text) { out << "${text}\n" }



out.write('-- BEGIN Handling time evaluator --\n')

log("Getting shipments from ${DAYS_BEFORE} days before")

def lastSentShipments = Shipment.withCriteria() {
    eq 'shippingMode', 'me2'
    isNotNull 'dateShipped'
    isNotNull 'dateReadyToShip'
    between 'dateShipped', new Date() - DAYS_BEFORE, new Date()
    'in' 'status', ['shipped','delivered','not_delivered']
    'in' 'senderId', [143554537L,99678614L,139720421L,143702240L,136289545L,131662738L,119541605L,
                        114349882L,133763301L,11567212L,145708592L,120009998L,146374410L,145166979L,
                        145999302L,70117681L,147859362L,130089245L,132961968L,118588816L,144865759L,
                        149254178L,119647379L,152125231L,151970801L,152788941L]
}

log("Processing ${lastSentShipments.size()} shipments")

lastSentShipments.groupBy{it.senderId}.each { senderId, shipments ->
  
    def redisHT = redisHandlingTimeService.get(senderId.toString())
    
    if (redisHT) {
        def daysDifference = shipments.collect{ computeHandlingTimeService.getWorkingDays(it.dateReadyToShip, it.dateShipped, it.siteId) }
      
        def avg = daysDifference.sum()/daysDifferece.size()
      
        log("User: ${senderId} - HT: ${redisHT} - Avg(last ${DAYS_BEFORE} days): ${avg}" )
        def diff = redisHT > avg ? redisHT - avg : avg - redisHT
        if (diff > 2) {
            log("[WARN]: differece between redisHT and avg is ${diff}")
        }
    } else {
        log("User: ${senderId} not found in redis")
    }
}

out.append('-- END Handling time evaluator --')