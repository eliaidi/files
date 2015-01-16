//Categorias padre que continen categorias fragiles
def rootCategories = ['MLA1002','MLA5726']

def mongoItemsService = ctx.getBean('mongoItemsService')
def mongoService = ctx.getBean('mongoService')
def redis = ctx.getBean('redisGeneralService')
def itemService = ctx.getBean('itemService')

out = new File('/tmp/items.log'); out.write('')
log = { text -> out << text + "\n" }

def CHUNK_SIZE = 50

List drop(List data, int size) {
    def output = []
    while (data && size--) {
        def elem = data.first()
        output << elem
        data.remove(elem)
    }
    return output
}

rootCategories.each { categId ->
  
  log "Getting tv categories from $categId.."
  def fragileCategories = mongoService.find(
    'categories', 
    [children_categories:[$size:0], path_from_root:[$elemMatch:[id:categId]], 'settings.fragile':true],
    ['id':1, '_id':0]
  ).collect{it.id}

  log "total fragile categories in $categId: ${fragileCategories.size()}" 

  log "Getting items.."
  def fragileItems = []
  def chunk = drop(fragileCategories, CHUNK_SIZE)
  while(chunk) {
      log "${fragileCategories.size()} remaining.."
      def result = mongoItemsService.find('items', 
                                        [category_id: [$in: chunk], 'shipping.mode':'me2', status:'active'], 
                                        ['id':1, 'seller_id':1, '_id':0])
      if (result)
        fragileItems << result
      chunk = drop(fragileCategories, CHUNK_SIZE)
      sleep(1000)
  }

  fragileItems = fragileItems.flatten()
  log "total fragile items:${fragileItems.size()}"

  def trustedUsers = redis.get("trusted_users")
  log "trustedUsers: $trustedUsers"

  def notAllowedFragileItems = fragileItems.findAll{!trustedUsers.contains(it.seller_id)}

  log "item ids:"
  fragileItems.groupBy{it.seller_id}.each{
    out << it.key + ": " + it.value.collect{it.id} + "\n"
  }

  log "items not allowed:"
  notAllowedFragileItems.each {
    log it.id
    //itemService.putItem([shipping:[mode:'not_specified', methods:[]]], it.id, it.seller_id)
  }
  log "done."
}