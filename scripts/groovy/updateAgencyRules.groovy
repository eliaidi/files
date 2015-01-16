def agenciesService = ctx.getBean('agenciesService')
def mongo = ctx.getBean('mongoCriticService')

out = new File('/tmp/out.log'); out.write('') //Archivo de log
log = { text -> out << text + "\n" }

log "Begin..."

def rules = mongo.find('agenciesRulesMLA',['to.imposition_center_id':"85", 'speed':[$gt:4]])

log "Size: ${rules.size()}"

rules.each { rule ->
  log "---------------------------------------------------------------------------"
  log "rule: ${rule._id} - from: ${rule.from.zip_code} to: ${rule.to.zip_code}"
  if (agenciesService.getAgencyByAgencyId(rule.from.imposition_center_id, "17500240").state_id in ["AR-B","AR-C"]) {
     mongo.update('agenciesRulesMLA', 
                  ['from.imposition_center_id': rule.from.imposition_center_id,
                   'to.imposition_center_id': rule.to.imposition_center_id,
                   'method_id': rule.method_id], 
                  [$set: [speed: 4, last_updated: new Date(), version: rule.version + 1]],
                 false,
                 false)
    log "rule: ${rule._id} - from: ${rule.from.zip_code} to: ${rule.to.zip_code} updated"
  }
  log "---------------------------------------------------------------------------"
}

log "End."