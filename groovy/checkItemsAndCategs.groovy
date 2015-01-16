import net.sf.json.groovy.JsonSlurper

restClient = ctx.getBean('restClient')
def file = new File('/tmp/itemsAndCategs.json')
def slurper = new JsonSlurper()
def json = slurper.parse(file)


def getCategoryFromAPI(categoryId) {
  def result
  restClient.get(
    uri: "/categories/$categoryId",
    success: {
      result = it.data
    },
    failure: {
      println "Error in API. Category: $categoryId"
    }
  )
  
  return result
}

def ok = 0
json.each { 
    
  def category = getCategoryFromAPI(it.category_id)
  if (category) {
    if (!(category.settings.shipping_modes.contains('me2'))) {
      println "Item mal publicado ${it.id} en ${it.category_id}"
    } else {
      ok++
    }
  } else {
    println "Error in item ${it.id} from category ${it.category_id}"
  }

  sleep(10)
}

println "fin - total: ${json.size()} - ok: $ok"
//println getCategoryFromAPI(json[0].category_id)