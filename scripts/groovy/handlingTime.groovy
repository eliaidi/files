import mercadoenvios.*
import java.math.*

//MLB
/*
--Muchos
6865759 963
38549536  781
30437647  741
42433693  617
36963339  608
96872105  477
*/
/*
--Medianos
37432853  101
33750182  100
*/
/*
--Pocos
70350715  19
30056009  19
*/
//MLA
/*
--Muchos
92607234  623
75338949  288
4641264 250
131662738 196
82916233  168
4542417 157
*/
/*
--Medianos
47277478  84
78568848  80
36484980  79
*/
/*
--Pocos
97013723  18
39542483  18
*/

def computeHandlingTimeService = ctx.getBean('computeHandlingTimeService')
def redisHandlingTimeService = ctx.getBean('redisHandlingTimeService')

//def users = [/*MLA*/92607234L, 4542417L, 82916233L, 79715665L, 21377353L,/*MLB*/38549536L, 85031997L, 71879567L]
//def users = [/*MLA*/92607234L, 4542417L]
//def users = [/*MLA*/82916233L, 79715665L, 21377353L]
//def users = [/*MLB*/38549536L, 85031997L, 71879567L]
//MLB
//def users = [6865759L,38549536L]
//def users = [30437647L,42433693L]
//def users = [36963339L,96872105L]
//def users = [37432853L,33750182L]
//def users = [70350715L,30056009L]
//MLA
//def users = [92607234L,75338949L,4641264L]
//def users = [131662738L,82916233L,4542417L]
//def users = [47277478L,78568848L,36484980L]
def users = [97013723L,39542483L]


def percentage(subtotal, total) {
   return Math.round(subtotal * 100 / total)
}

def shipmentsByUser = Shipment.withCriteria {
          isNotNull 'dateReadyToShip'
          eq 'shippingMode', 'me2'
          between 'dateShipped', new Date() - 15, new Date()
          'in' 'senderId', users
        }.groupBy { it.senderId }


shipmentsByUser.each { userId, shipments ->
    
    def estimatedHT = redisHandlingTimeService.get(userId.toString())
  
    if (estimatedHT) {
      def differences = shipments.collect{ estimatedHT - computeHandlingTimeService.getWorkingDays(it.dateReadyToShip, it.dateShipped, it.siteId) }
      def positives = differences.findAll{ it >= 0 }
      def negatives = differences.findAll{ it < 0 }.collect{-it}
  
      println "\nUser: $userId"
      
      println "HT: $estimatedHT"
      println "Total: ${shipments.size()}"
      println "Bien: ${percentage(positives.size(),shipments.size())}%"
      println " max: ${positives.max()}"
      println " min: ${positives.min()}"
      println " avg: ${positives.sum()/positives.size()}"
      if (negatives) {
        println "Mal: ${percentage(negatives.size(),shipments.size())}%"
        println " max: ${negatives.max()}"
        println " min: ${negatives.min()}"
        println " avg: ${negatives.sum()/negatives.size()}"
      }
      
    } else {
       println "\nUser $userId not found in redis"
    }
  
}

def nothing = []