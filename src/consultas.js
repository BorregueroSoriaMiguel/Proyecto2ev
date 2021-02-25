/*Lo que queremos obtener es el id del equipo,
junto a su nombre y todas las competiciones que ha ganado.*/
db.equipos.aggregate([
    {
        $addFields: { obj: { k:"$nombre_equipo", v:"$palmares" } }
    },
    {
        $group: { _id:"$id_equipo", datos: { $push: "$obj" } }
    },
    {
        $project: {
            array_palmares_y_equipo:{
                $concatArrays:[
                    [ { "k":"_id", "v":"$_id" } ],
                    "$datos"
                ]
            }
        }
    },
    {
        $replaceWith: { $arrayToObject:"$array_palmares_y_equipo" }
    },
    {
        $sort: { _id:1 }
    }
])
db.equipos.aggregate([
    {
        $project:{
            _id:0,
            id_equipo:1,
            nombre_equipo:1,
            palmares:1,
        }
    }
])



/*Con esta consulta obtenemos los equipos y las competiciones internacionales
en las que están participando los equipos siempre que sean los reyes de alguna competición.*/
db.equipos.aggregate([
    {
        $unwind: "$id_competicion"
    },
    {
        $match:{
            $and:[
                { $or:[
                    {"id_competicion":1},
                    {"id_competicion":2 }
                ] },
                {"rey.confirm":true}
            ]
        }
    },
    {
        $lookup:{
            from:"competiciones",
            localField:"id_competicion",
            foreignField:"id_competicion",
            as:"participa"
        }
    },
    {
        $project:{
            "_id":0,
            "id_competicion":0,
            "palmares":0,
            "rey":0,
            "participa._id":0
        }
    },
    {
        $sort: { id_equipo:1 }
    }
]).pretty()



/*Con este conjunto de aggregates conseguimos acceder a todos los
datos de la basede datos y obtener el valor total de todos los equipos.*/
db.plantillas_y_traspasos.drop()
db.plantillas.aggregate([
    {
        $lookup:{
            from:"traspasos",
            localField:"id_integrante",
            foreignField:"id_integrante",
            as:"plantillas_y_traspasos"
        }
    },
    {
        $project:{
            "_id":0
        }
    },
    {
        $merge: {
            into: "plantillas_y_traspasos"
        }
    }
])
db.equipos_y_competiciones.drop()
db.equipos.aggregate([
    {
        $unwind: "$id_competicion"
    },
    {
        $lookup:{
            from:"competiciones",
            localField:"id_competicion",
            foreignField:"id_competicion",
            as:"participa"
        }
    },
    {
        $project:{
            "_id":0
        }
    },
    {
        $merge: {
            into: "equipos_y_competiciones"
        }
    }
]).pretty()
db.allcoll.drop()
db.equipos_y_competiciones.aggregate([
    {
        $lookup:{
            from:"plantillas_y_traspasos",
            localField:"id_equipo",
            foreignField:"id_equipo",
            as:"allcoll"
        }
    },
    {
        $project:{
            "_id":0,
            "id_equipo":1,
            "nombre_equipo":1,
            "fecha_fundado":1,
            "palmares":1,
            "participa.id_competicion":1,
            "participa.nombre_competicion":1,
            "rey":1,
            "allcoll.id_integrante":1,
            "allcoll.nombre":1,
            "allcoll.apellidos":1,
            "allcoll.fecha_nacimiento":1,
            "allcoll.tipo":1,
            "allcoll.valor":1,
            "allcoll.plantillas_y_traspasos":1,
            "valor_equipo":1
        }
    },
    {
        $set:{
            valor_equipo: { $sum: "$allcoll.valor" }
        }
    },
    {
        $sort:{ id_equipo: 1, nombre_equipo:1}
    }
]).pretty()



//Queremos saber quienes son la o las promesas de fútbol y a que equipo pertenecen.
db.plantillas.aggregate([
    {
        $lookup:{
            from:"equipos",
            localField:"id_equipo",
            foreignField:"id_equipo",
            as:"plantillas_y_equipos"
        }
    },
    {
        $match:{
            $and:[
                 { "tipo":"jugador" },
                 { $expr: { $gte: [ { $year: "$fecha_nacimiento" },1998 ] } },
                 { $expr: { $gte: [ "$valor", 30000000 ] } } 
            ]
        }
    },
    {
        $project:{
            "_id":0,
            "id_integrante":1,
            "nombre":1,
            "apellido":1,
            "edad": {
                $subtract:[
                    2021,
                    { $year:"$fecha_nacimiento" }
                ]
            },
            "valor":1,
            "plantillas_y_equipos.id_equipo":1,
            "plantillas_y_equipos.nombre_equipo":1
        }
    }
]).pretty()



/*Con esta consulta obtenemos los equipos que participan
en la Champions,LaLiga Santander y en la Copa del Rey.*/
db.equipos.aggregate([
    {
        $project:{
           "_id":0,
            "nombre_equipo":1,
            "id_champions": { $eq: [ { $arrayElemAt: [ "$id_competicion", 0 ] }, 1 ] },
            "id_laliga_sananter": { $eq: [ { $arrayElemAt: [ "$id_competicion", 1 ] }, 3 ] },
            "id_copa_del_rey": { $eq: [ { $arrayElemAt: [ "$id_competicion", 2 ] }, 5 ] },
        }
    },
    {
        $match:{
            $expr: { $eq: [ "$id_champions", true ] },
            $expr: { $eq: [ "$id_laliga_santander", true ] },
            $expr: { $eq: [ "$id_copa_del_rey", true ] }
        }
    }
])