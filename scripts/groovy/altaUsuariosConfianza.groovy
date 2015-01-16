def redis = ctx.getBean("redisGeneralService")

//------------- VER USUARIOS ---------------------------
/*
def whiteList = redis.get("trusted_users")
println "users: $whiteList"
*/

//------------- AGREGAR USUARIOS ------------------------

def userIds = [145797961,77581040]

//Obtengo usuarios
def tu = redis.get("trusted_users")
println "users: $tu"

if (tu)
  newTu = [] + tu + userIds
else
  newTu = userIds

//Agrego nuevo usuario
redis.set("trusted_users", newTu, true)

//Veo nueva lista
def users = redis.get("trusted_users")
println "users: $users"

//------------- ELIMINAR USUARIO ------------------------
/*
def userId = 138398894

//Obtengo usuarios
def tu = redis.get("trusted_users")
println "users: $tu"

if (tu) {
  newTu = [] + tu - userId

  //Agrego nuevo usuario
  redis.set("trusted_users", newTu, true)
}

//Veo nueva lista
def users = redis.get("trusted_users")
println "users: $users"
users.find{it == userId}? "NO" : "OK"
*/