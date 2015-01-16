import net.sf.json.groovy.JsonSlurper

restClient = ctx.getBean("restClient")

out = new File("/tmp/categs.log"); out.write("")

def log(String text){
 out << text + "\n" 
}

def getCategory(categId){
  def data
  
  restClient.get(
    uri: "/categories/${categId}",
    success: {
      data = it.data
    },
    failure: {
      log "failure ${categId}"
    }
  )
  return data
}

def searchCategories(categoryId) {

  def category = getCategory(categoryId)

  category.children_categories.each { child ->
    
    if (child.children_categories.size() > 0)
      searchCategories(child.id)      
    else
      log "C: ${child.id}:${child.settings.shipping_modes}"
  }
}

searchCategories("MLA1002")