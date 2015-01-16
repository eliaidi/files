import mercadoenvios.*
import static java.lang.Math.*
  
def rest = ctx.getBean('restClient')
def adoptionService = ctx.getBean('adoptionService')
def itemService = ctx.getBean('itemService')
def memcachedUsersWrapperService = ctx.getBean('memcachedUsersWrapperService')


def cambiar = { userId ->
  rest.put uri:"/users/$userId?caller.scopes=admin",
  data:[shipping_modes:['custom','not_specified','me1']],
  success: {
    println "-"*50
    println it.data

    def a = Adoption.get(userId)
    a.userId = userId
    a.option = 'in'
    a.shippingMode='me1'
    a.siteId = 'MLB'
    a.status = 'pending'
    if (a.save()){
      memcachedUsersWrapperService.delete "PREFERENCE-$userId"
      memcachedUsersWrapperService.delete "PREFERENCE-FULL-$userId"
      println "Adoption OK"
    }else{
      println a.errors
    }
  },
  failure: {
    println "ERROR on user: $it.data"
  }
}

def processItems = { userId ->
    def itemIds = adoptionService.getAllItemsUser(userId)
    itemIds.each{ iid ->
      def i = itemService.getItemData(iid)
      if (i.shipping.mode == 'me2'){
        println "$iid :: $i.permalink $i.permalink -> $i.shipping"
        def dimensions = i.shipping.dimensions.split(',')
        def weight = dimensions[1]
        def dims = dimensions[0].split('x').sort{it}
        def fdims = [max(dims[0] as Integer, 16), max(dims[1] as Integer, 11), max(dims[2] as Integer, 2)]
        def sfdim = "${fdims.join('x')},$weight"
        String uri = "/items/${iid}?caller.scopes=admin&client.id=1360974954364596"
        rest.put uri: uri,
          data: [shipping:[mode:'me1', dimensions:sfdim.toString()]],
          success:{
            println "$iid OK"
            println "-"*50
          },
          failure:{
            println "ERROR on $iid $it.data"
          }
      }
    }
}

//Cambios...
cambiar(119583463L)
processItems(119583463L)
''
