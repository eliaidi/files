import mercadoenvios.Adoption
import mercadoenvios.AdoptionLog

memcached = ctx.getBean('memcachedUsersWrapperService')

//TODO chequear que sean 'in' de 'me2' de 'MLA', sino adaptar script
def userIds = [123,456]

userIds.each { userId ->

  def adoption = Adoption.findByUserId(userId)
  adoption.option = 'trial'
  adoption.save(flush:true, failOnError: true)
  
  def adoptionLog = new AdoptionLog()
  adoptionLog.userId = userId
  adoptionLog.option = 'trial'
  adoptionLog.siteId = 'MLA'
  adoptionLog.show = true
  adoptionLog.shippingMode = 'me2'
  adoptionLog.save(flush:true, failOnError: true)
  
  println memcached.delete("PREFERENCE-FULL-${userId}")
  println memcached.delete("PREFERENCE-${userId}")
}