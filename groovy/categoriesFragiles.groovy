import net.sf.json.groovy.JsonSlurper

restClient = ctx.getBean("restClient")
total = 0
fragil = 0
out = new File("/tmp/hojasFragiles.sh"); out.write("")
categs = ["MLA123859","MLA123860","MLA123861","MLA123862","MLA123863","MLA125067","MLA124821","MLA124839","MLA124857","MLA124875","MLA124893","MLA124983","MLA10557","MLA10561","MLA14871","MLA14874","MLA14936","MLA14937","MLA81532","MLA81533","MLA81534","MLA81535","MLA81536","MLA81539","MLA11899","MLA14941","MLA123864","MLA123865","MLA123866","MLA123867","MLA124911","MLA124929","MLA124947","MLA124965","MLA10570","MLA14885","MLA14904","MLA78918","MLA81537","MLA81538","MLA81990","MLA82030","MLA11902","MLA14940","MLA14942","MLA78913"]
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
  total++
  def category = getCategory(categoryId)
  
  if (category.children_categories)
    category.children_categories.each { child ->
      searchCategories(child.id)
    }
  else
    if (!category.settings.shipping_modes.contains("me2")) {
      fragil++
      log '''curl -i -X PUT -H "Content-Type: application/json" -d '{"shipping_modes":["me1","me2","custom","not_specified"], "dimensions":{"height":70, "width":70, "length":70, "weight":6200}, "fragile":true}' 'internal.mercadolibre.com/internal/categories/''' + category.id + "'"
    }

}

categs.collect{searchCategories(it)}

println "total: $total"
println "fragil: $fragil"