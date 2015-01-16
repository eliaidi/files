import mercadoenvios.*
  
def rest = ctx.getBean('restClient')
def memcachedUsersWrapperService = ctx.getBean('memcachedUsersWrapperService')
  
def cambiarAddress={userId, zipCode, address, number, complement ->
  
  rest.get uri: "/countries/BR/zip_codes/$zipCode".toString(),
  success:{
    def url = "/addresses?caller.id=$userId".toString()
    def addressLine = "$address $number" + (complement?", $complement":"")
    def body = [address_line: addressLine.toString(),
                 street_name:address,
                 street_number:number,
                 zip_code: zipCode,
                 city:[
                   id:it.data.city.id, 
                   name: it.data.city.name?:(it.data.extended_attributes?.city_name)
                 ],
                 state: it.data.state,
                 country: it.data.country,
                 types:['default_selling_address'],
                 comment:complement
                ]
    println "POST $url"
    println body
    println "-"*50
    rest.post uri: url, data: body, headers:["Encoding" : "UTF-8"],
      success: {
        println it.data
      },
        failure:{
          println it.data
        }
    println "."*50
  }
}

def optin = { userId ->
  rest.put uri:"/users/$userId?caller.scopes=admin",
  data:[shipping_modes:['custom','not_specified','me1']],
  success: {
    println "-"*50
    println it.data
    if (!Adoption.exists(userId)){
      def a = new Adoption()
      a.userId = userId
      a.option = 'in'
      a.shippingMode='me1'
      a.siteId = 'MLB'
      a.freeMethod = 100009L
      a.status = 'pending'
      if (a.save()){
        memcachedUsersWrapperService.delete "PREFERENCE-$userId"
        memcachedUsersWrapperService.delete "PREFERENCE-FULL-$userId"
        println "Adoption OK"
      }else{
        println a.errors
      }
    }else{
      println "Warning, user already IN"
    }
  },
  failure: {
    println "ERROR on user: $it.data"
  }
}

//Cambios...
//def cambiarAddress={userId, zipCode, address, number, complement ->
// cambiarAddress(145314143L, '03029010', 'Rua Dr. Pacheco e Silva', 110, ''); 
optin(30934674L);