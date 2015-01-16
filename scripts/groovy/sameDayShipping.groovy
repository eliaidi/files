import mercadoenvios.*
import categories.*
import grails.converters.JSON
import mercadoenvios.utils.*
import net.sf.json.groovy.JsonSlurper

def CHUNK_SIZE = 100

List drop(List data, int size) {
    def output = []
    while (data && size--) {
        def elem = data.first()
        output << elem
        data.remove(elem)
    }
    return output
}

out = new File('/tmp/sameDayShipping.log')
log = { text -> out << text + "\n" }
log "--------------------------- BEGIN --------------------------"


//USERS
log "Getting and processing users..."
def usersMoto = [ "MLA": ShippingPreference.findAllByShippingServiceId(81L).collect{it.userId} ]

log "Total users found: ${usersMoto.MLA.size()}"
def usersOut = new File('/tmp/sameDayShipping_users.json')
usersOut.write((usersMoto as JSON).toString())


//CATEGORIES
log "Getting categories..."
def categories = DimensionCategory.withCriteria {
	le 'dimensionMid', 40D
	le 'dimensionMax', 40D
	le 'dimensionMin', 40D
	le 'weight', 11000D
	projections {
		property 'categoryId'
	}
}
categories = categories.findAll{it[0..2] == 'MLA'}
mongoService = ctx.getBean('mongoService')

//Devuelve las categorias hoja del categoryId especificado
getLeafCategories = { categoryId ->
	def category = mongoService.find("categories", [id: categoryId])
    def children = category.children_categories.find{it}
    
	return children?.size() > 0 ?
      children.collect { getLeafCategories(it.id) } :
      category.id
}

log "Processing categories..."
log "Categories found in DB: ${categories.size()}"

//Proceso las categorias por chunks, para no sobrecargar lectura en mongo
def chunkedLeafCategories = []
def chunk = drop(categories, CHUNK_SIZE)
while(chunk) {
    log "${categories.size()} categories remaining..."
    
    chunkedLeafCategories << chunk.collect{ getLeafCategories(it) }
	chunk = drop(categories, CHUNK_SIZE)
    sleep(100)
}

//Total de categorias hoja (hay repetidas)
log "Total chunkedLeafCategories: ${chunkedLeafCategories.flatten().size()}"
def totalLeafCategories = chunkedLeafCategories.flatten().unique()

//Total sin repeticiones
log "Total leaf categories found: ${totalLeafCategories.size()}"
def categoriesMoto = [ 'MLA': totalLeafCategories ]

def categoriesOut = new File('/tmp/sameDayShipping_categories.json')
categoriesOut.write((categoriesMoto as JSON).toString())

log "done."