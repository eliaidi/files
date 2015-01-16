import mercadoenvios.*
import mercadoenvios.utils.JsonUtil  
import grails.converters.JSON
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.DateTime

Date parseDate(String date) {
  ISODateTimeFormat.dateTimeParser().parseDateTime(date).toDate()
}

out = new File('/tmp/out2.log'); out.write('')
log = { text -> out << text + "\n" }

estornados = JSON.parse(new File('/tmp/estornados.json').getText())

def results = [:]
def aboveAvg = 0
def unnecesary = 0
def twoMailsShipped = 0
try {
  log "estornados: ${estornados.size()}"
  estornados.each { tn, events ->
    
    def estornado = events.find{it.code.toUpperCase().startsWith("EST")}
    def idx = events.indexOf(estornado)
    if (++idx < events.size()) {
        def previous = events[idx]
        def timeDiff = (parseDate(estornado.date).getTime() - 
                        parseDate(previous.date).getTime()) / 1000 / 60
        if (results[(estornado.code)]) {
            results[(estornado.code)].n++
            results[(estornado.code)].diff += timeDiff
        } else {
            results[(estornado.code)] = [n: 1, diff: timeDiff, codes: new HashMap<String,List<String,Integer>>()]
        }
        if (estornado.code == "EST09") {
          if (timeDiff >= 60)
            aboveAvg++ 
          def forEstornados = events.findAll{it.code == previous.code}
          if (forEstornados.size() >= 2) {
            unnecesary++
            log "forEstornados: $forEstornados"
            //def postEstornado = forEstornados.findAll{parseDate(it.date) > parseDate(estornado.date)}.sort{parseDate(it.date)}.first()
            //if (parseDate(postEstornado.date) - parseDate(estornado.date) >= 1)
              //twoMailsShipped++
          }
        }
        if (results[(estornado.code)].codes[(previous.code)])
          results[(estornado.code)].codes[(previous.code)][1]++
        else
          results[(estornado.code)].codes[(previous.code)] = [previous.description, 1]
    }
  }
} catch (Exception e) {
  log "Exception: ${e.getMessage()} - ${e.getCause()} - ${e.getStackTrace()}"
}

results.each { code, calcs ->
  log "$code: ${calcs.n} - ${((calcs.diff/calcs.n) as float).round(0)} - ${calcs.codes.sort{a,b -> b.value[1] <=> a.value[1]}}"
}
log "aboveAvg: $aboveAvg"
log "unnecesary: $unnecesary"