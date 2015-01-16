import mercadoenvios.*
import org.hibernate.FetchMode
import org.hibernate.transform.DistinctRootEntityResultTransformer
out = new File('/tmp/deletedEvents.log')//; out.write('')
log = { text -> out << text + "\n" }
mongoService = ctx.getBean('mongoService')

def pending = mongoService.find('rollbackEvents', [status: 'pending'])
def rollback = mongoService.find('rollbackEvents', [status: 'rollback'])
def ignore = mongoService.find('rollbackEvents', [status: 'ignore'])

println "pending"
println pending.size()
println "rollback"
println rollback.size()
println "ignore"
println ignore.size()

def ok = []; def wrong = []; def noEst = []
rollback.each{
  def sId = it.shipping_id
  def s = Shipment.createCriteria().get{
    eq 'id', sId
    fetchMode 'trackings', FetchMode.JOIN
    resultTransformer(new DistinctRootEntityResultTransformer())
  }
  
  def est09 = s.trackings.find{it.trackingStatus == 'EST09'}
  if (est09) {
    if (s.status in ['pending','ready_to_ship'])
      ok << s.trackingNumber
    if (s.status == 'shipped')
      wrong << s.trackingNumber
  } else {
    noEst << s.trackingNumber
  }
}
println "ok: ${ok.size()} $ok"
println "wrong: ${wrong.size()} $wrong"
println "noEst: ${noEst.size()} $noEst"