import mercadoenvios.*
import static mercadoenvios.constants.ShippingConstants.*

userId = 59041280L //TODO completar con custId
json = [shipping: [
                   free_shipping: true,
                   mode: SHIPPING_MODE_ME2, 
                   methods:[[id: 100009L, free: SHIP_METHOD_FREE_COUNTRY]] 
                  ]
       ]

itemService = ctx.getBean('itemService')

def itemIds = ['MLB527220455','MLB527603825','MLB527603826','MLB527603829',
               'MLB527603830','MLB527607411','MLB527607412','MLB527607413',
               'MLB527607415','MLB527603831','MLB527603832','MLB527603832',
               'MLB527603836','MLB527603837','MLB527607417','MLB527607418',
               'MLB527607419','MLB527607420','MLB527607421','MLB527607422',
               'MLB527607423','MLB527603838','MLB527603840','MLB527603841',
               'MLB527603842','MLB527603844','MLB527607425','MLB527607426',
               'MLB527607427','MLB527603843','MLB527603845','MLB527603847',
               'MLB527607428','MLB527607429','MLB527607430','MLB527607431',
               'MLB527603852','MLB527603853','MLB527607432','MLB527603945',
               'MLB528061777','MLB528150661','MLB528159849','MLB528165733',
               'MLB528167612','MLB528168448','MLB528169323','MLB528246925',
               'MLB528244652','MLB521613833','MLB521609292','MLB521609326',
               'MLB522835861','MLB522835872','MLB523953792','MLB517015349',
               'MLB526709330','MLB523953793','MLB526169481','MLB517422683',
               'MLB517422871','MLB517011831']
  
def marcados = []
itemIds.each { itemId ->
    def item = itemService.getItemData(itemId, userId)
  
    if(PUBLICATION_FREE != item.listing_type_id
    && PUBLICATION_PAY_MODE != item.buying_mode) {

      def response = itemService.putItem(json, item.id, userId)
      if (response.status == 200)
        marcados << item.id
      else
        println "falló el put de ${item.id}"
    } else {
      println "${item.id} ignorado: ${item.listing_type_id} ${item.buying_mode} ${item.status}"
    }
}

println "marcados: $marcados"