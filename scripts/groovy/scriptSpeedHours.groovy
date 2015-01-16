import mercadoenvios.*
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import groovyx.gpars.*
import java.math.*
import groovy.time.TimeCategory

nonWorkingDays = ["MLB":["12/25/12", "12/31/12", "01/01/13", "02/12/13", "03/29/13", "04/21/13",
                         "05/01/13", "05/30/13", "11/15/13", "12/25/13", "11/25/13", "07/09/13", "11/20/13"],
                  "MLA":["01/01/13", "01/31/13", "02/11/13", "02/12/13", "02/20/13", "03/24/13",
                         "03/29/13", "04/01/13", "04/02/13", "05/01/13", "05/25/13", "06/20/13", "06/21/13",
                         "07/09/13", "08/19/13", "10/14/13", "11/25/13", "12/08/13", "12/25/13"]]


def shipment = [dateShipped: Date.parse("dd/MM/yy hh","22/11/13 05"), dateDelivered: Date.parse("dd/MM/yy hh","25/11/13 10")]
//20817361027 - 20756232326
//def shipment = Shipment.get(20817361027)

println shipment.dateShipped
println shipment.dateDelivered

println getWorkingHours(shipment.dateShipped, shipment.dateDelivered, "MLA")


def getWorkingHours(start, end, siteId) {

  def holidays = nonWorkingDays[siteId].collect { new Date(it) }.findAll { !(it.toCalendar()[Calendar.DAY_OF_WEEK] in [1, 7]) }
  def weekMap = [ 1:-1, 2:0, 3:1, 4:2, 5:3, 6:4, 7:-2 ]

  def w1 = weekMap[start.toCalendar()[Calendar.DAY_OF_WEEK]]
  def ww1 = w1>0?w1:0
  def c1 = start - w1
  
  def w2 = weekMap[end.toCalendar()[Calendar.DAY_OF_WEEK]]
  def ww2 = w2>0?w2:0
  def c2 = end - w2
  
  long days = c2 - c1
  long weekendDays = (days/7)*2
  long noWE = days-weekendDays
  long hol = holidays.findAll {it > start && it < end}.size()
  
  h1 = c1.toCalendar().get(Calendar.HOUR_OF_DAY)
  h2 = c2.toCalendar().get(Calendar.HOUR_OF_DAY)
  
  return ((noWE-ww1+ww2-hol)*24)-h1+h2
}
