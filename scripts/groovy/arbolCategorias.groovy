import mercadoenvios.*
import static groovyx.gpars.GParsPool.withPool
  
categoriesService = ctx.getBean('categoriesService')
dcService = ctx.getBean('dimensionsCategoriesService')
restClient = ctx.getBean('restClient')
out = new File('/tmp/dimcat.out')

def getL1(def siteId) {
  def output
  restClient.get(
    uri: "/sites/$siteId/categories",
    success: {output = it.data},
    failure: {println "CALL FAILED"}
  )
  
  return output
}

def isME2(def category) {
  return 'me2' in category.settings.shipping_modes
}

def verifyDimensions(def category) {
  def isLeaf = category.children_categories.isEmpty()
  def isMe2 = isME2(category)
  def hasDC = dcService.getDimensionsCategory(category.path_from_root) != null
  if (isLeaf && isMe2 && !hasDC) {
    out << category.id + "\n"
    return []
  } else if (hasDC) {
    out << "${category.id} is OK\n"
    return []
  } else
    return category.children_categories*.id
}

def process() {
  Queue q = new LinkedList()
  String siteId = 'MLA'
  getL1(siteId)?.each{q.add(it.id)}
  
  while(!q.isEmpty()) {
    def category = categoriesService.getUncachedCategory(q.poll())
    q.addAll(verifyDimensions(category))
  }
}

process()