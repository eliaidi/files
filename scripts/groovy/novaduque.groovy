import org.codehaus.groovy.grails.commons.ConfigurationHolder as CH
import mercadoenvios.*
import mercadoenvios.conciliation.*
import org.hibernate.FetchMode
import groovy.xml.StreamingMarkupBuilder


out = new File('/tmp/novaduque.log')
out << "-------------------------------------------------------------------------\n"
usersApiService = ctx.getBean('usersApiService')

sentShipments = [21004167154,21004244815,21004240732,21004160869,21004255779,21004831447,
                 21004612388,21004599175,21004594419,21004776600,21004587605,21004673037,21004591446]

def restConnector = ctx.getBean('restConnector')
def configService = ctx.getBean('configService')
  
//CH.config.novaduque.template = "http://#{clientCode}.novaduque.com.br/envio/xml/#{password}"
CH.config.novaduque.template = "http://teste.kaisari.com.br/#{clientCode}/envio/xml/#{password}"

def client = "meli"
def pass = "970cc5c0-641e-11e3-949a-0800200c9a66"


def getNovaduqueMockObject() {
      out << "Getting shipments...\n"
      def realShipmentsData = RealShipmentData.withCriteria {
        shipment {
                  between 'dateShipped', new Date() - 3, new Date()
                  eq 'siteId', 'MLB'
                  eq 'status', 'not_delivered'
            }
      }
      out << "Sorting...\n"
      def rsd = realShipmentsData.sort{a, b -> a.shipment.dateCreated <=> b.shipment.dateCreated}?.findAll{!sentShipments.contains(it.shipment.id)}.last()
      def sender = usersApiService.getUser(rsd.shipment.senderId)
      
      out << "Shipment selected: ${rsd.shipment.id}" + "\n"
      return [
               [
                  data_envio: rsd.shipment.dateShipped.format('yyyy-MM-dd'),
                  hora_envio: rsd.shipment.dateShipped.format('k:mm'),
                  registro: rsd.shipment.trackingNumber,
                  peso: rsd.weight as Integer,
                  v_postal: rsd.amountCharged,
                  valor_dec: rsd.amountReceivable,
                  ar: 'N',
                  dh: 'N',
                  remetente: sender.first_name + " " + sender.last_name,
                  cpf_cnpj: sender.identification.number,
                  cep_r: rsd.shipment.senderAddress.zipCode,
                  endereco_r: rsd.shipment.senderAddress.streetName,
                  numero_r: rsd.shipment.senderAddress.streetNumber,
                  cidade_r: rsd.shipment.senderAddress.cityName,
                  uf_r: rsd.shipment.senderAddress.stateId[-2..-1],
                  destinatario: rsd.shipment.contact,
                  cep: rsd.shipment.receiverAddress.zipCode,
                  endereco: rsd.shipment.receiverAddress.streetName,
                  numero: rsd.shipment.receiverAddress.streetNumber,
                  cidade: rsd.shipment.receiverAddress.cityName,
                  uf: rsd.shipment.receiverAddress.stateId[-2..-1]
               ]
            ]
}

def createXML(List<Map> objects) {
  def smb = new StreamingMarkupBuilder()
  return smb.bind {
    objetos {
      objects.each { obj ->
        objeto {
          data_envio(obj.data_envio)
          hora_envio(obj.hora_envio)
          registro(obj.registro)
          peso(obj.peso)
          v_postal(obj.v_postal)
          valor_dec()
          ar(obj.ar)
          ag()
          dh(obj.dh)
          remetente(obj.remetente)
          cpf_cnpj(obj.cpf_cnpj)
          cep_r(obj.cep_r)
          endereco_r(obj.endereco_r)
          numero_r(obj.numero_r)
          complemento_r()
          bairro_r()
          cidade_r(obj.cidade_r)
          uf_r(obj.uf_r)
          telefone_r()
          destinatario()
          cep(obj.cep)
          endereco(obj.endereco)
          numero(obj.numero)
          complemento()
          bairro()
          cidade(obj.cidade)
          uf(obj.uf)
          telefone()
        }
      }
    }
  }.toString()
}

def novaduqueMockObject = getNovaduqueMockObject()
out << "nova: $novaduqueMockObject" + "\n"

String url = configService.template("novaduque.template", [clientCode: client, password: pass])
String body = createXML(novaduqueMockObject)
out << url+"\n"
out << body+"\n"

def MAX_RETRIES = 3
for (int retries = 0; retries < MAX_RETRIES; retries++) {
      def response = restConnector.execPost(url, body, "xml")
      if (response.status == 200) {
            try {
                  out << "XML response: " + new XmlParser().parseText(response.data) + "\n"
            } catch(Exception e) {
                  out << "Error con el XML retornado - E: $e" + "\n"
            }
            break;
      }  
}
