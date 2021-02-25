db.competiciones.drop()
db.competiciones.insertMany([
    {
        "id_competicion":01,
        "nombre_competicion":"Champions"
    },
    {
        "id_competicion":02, "nombre_competicion":"UEFA"
    },
    {
        "id_competicion":03, "nombre_competicion":"LaLiga Santander"
    },
    {
        "id_competicion":04, "nombre_competicion":"LaLiga SmartBank"
    },
    {
        "id_competicion":05, "nombre_competicion":"Copa del Rey"
    },
])
db.equipos.drop()
db.equipos.insertMany([
    {
        "id_equipo":01,
        "id_competicion":[1,3,5],
        "nombre_equipo":"Sevilla Fútbol Club",
        "fecha_fundado":ISODate("1890-01-25"),
        "palmares":{"UEFA":6,"LaLiga Santander":1,"LaLiga SmartBank":4,"Copa del Rey":5},
        "rey":{"confirm":true, "competicion":["UEFA"]}
    },
    {
        "id_equipo":02, "id_competicion":[3], "nombre_equipo":"Real Betis Balompié", "fecha_fundado":ISODate("1907-09-12"), "palmares":{"LaLiga Santander":1,"LaLiga SmartBank":7,"Copa del Rey":2}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":03, "id_competicion":[4], "nombre_equipo":"Málaga Club de Fútbol", "fecha_fundado":ISODate("1948-05-25"), "palmares":{"LaLiga SmartBank":4}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":04, "id_competicion":[1,3,5], "nombre_equipo":"Fútbol Club Barcelona", "fecha_fundado":ISODate("1899-11-29"), "palmares":{"Champions":5,"LaLiga Santander":26,"Copa del Rey":30}, "rey":{"confirm":true, "competicion":["Copa del Rey"]}
    },
    {
        "id_equipo":05, "id_competicion":[1,3], "nombre_equipo":"Club Atlético de Madrid", "fecha_fundado":ISODate("1903-04-26"), "palmares":{"UEFA":3,"LaLiga Santander":10,"Copa del Rey":10}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":06, "id_competicion":[3], "nombre_equipo":"Real Club Celta de Vigo", "fecha_fundado":ISODate("1923-08-23"), "palmares":{"LaLiga SmartBank":4}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":07, "id_competicion":[3,5], "nombre_equipo":"Athletic Club", "fecha_fundado":ISODate("1898-01-01"), "palmares":{"LaLiga Santander":8,"Copa del Rey":23}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":08, "id_competicion":[2,3], "nombre_equipo":"Real Sociedad de Fútbol", "fecha_fundado":ISODate("1909-09-07"), "palmares":{"LaLiga Santander":2,"LaLiga SmartBank":3,"Copa del Rey":2}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":09, "id_competicion":[2,3], "nombre_equipo":"Villarreal Club de Fútbol", "fecha_fundado":ISODate("1923-01-01"), "palmares":{}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":10, "id_competicion":[2,3], "nombre_equipo":"Granada Club de Fútbol", "fecha_fundado":ISODate("1931-04-06"), "palmares":{"LaLiga SmartBank":3}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":11, "id_competicion":[3,5], "nombre_equipo":"Levante Unión Deportiva", "fecha_fundado":ISODate("1909-09-09"), "palmares":{"LaLiga SmartBank":2}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":12, "id_competicion":[1,3], "nombre_equipo":"Real Madrid Club de Fútbol", "fecha_fundado":ISODate("1902-03-06"), "palmares":{"Champions":13,"UEFA":2,"LaLiga Santander":34,"Copa del Rey":19}, "rey":{"confirm":true, "competicion":["Champions","LaLiga Santander"]}
    },
    {
        "id_equipo":13, "id_competicion":[4], "nombre_equipo":"Albacete Balompié", "fecha_fundado":ISODate("1940-08-01"), "palmares":{"LaLiga SmartBank":1}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":14, "id_competicion":[4], "nombre_equipo":"Unión Deportiva Almería", "fecha_fundado":ISODate("1989-07-26"), "palmares":{}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":15, "id_competicion":[4], "nombre_equipo":"Unión Deportiva Las Palmas", "fecha_fundado":ISODate("1949-08-22"), "palmares":{"LaLiga SmartBank":4}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":16, "id_competicion":[4], "nombre_equipo":"Real Oviedo", "fecha_fundado":ISODate("1926-03-26"), "palmares":{"LaLiga SmartBank":5}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":17, "id_competicion":[4], "nombre_equipo":"Rayo Vallecano de Madrid", "fecha_fundado":ISODate("1924-05-29"), "palmares":{"LaLiga SmartBank":1}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":18, "id_competicion":[4], "nombre_equipo":"Real Zaragoza", "fecha_fundado":ISODate("1932-03-18"), "palmares":{"LaLiga SmartBank":1,"Copa del Rey":6}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":19, "id_competicion":[3], "nombre_equipo":"Valencia Club de Fútbol", "fecha_fundado":ISODate("1919-03-18"), "palmares":{"UEFA":1,"LaLiga Santander":6,"LaLiga SmartBank":2,"Copa del Rey":8}, "rey":{"confirm":false, "competicion":[]}
    },
    {
        "id_equipo":20, "id_competicion":[3], "nombre_equipo":"Cádiz Club de Fútbol", "fecha_fundado":ISODate("1910-09-10"), "palmares":{"LaLiga SmartBank":1}, "rey":{"confirm":false, "competicion":[]}
    }
])

db.plantillas.drop()
db.plantillas.insertMany([
    {
        "id_integrante":01,
        "id_equipo":01,
        "tipo":"entrenador",
        "nombre":"Julen",
        "apellido":"Lopetegui",
        "fecha_nacimiento":ISODate("1977-05-02"),
        "valor":20000000
    },
    {
        "id_integrante":02, "id_equipo":01, "tipo":"medico","nombre":"Juan", "apellido":"Fernández", "fecha_nacimiento":ISODate("1983-01-21"), "valor":30000
    },
    {
        "id_integrante":03, "id_equipo":01, "tipo":"jugador","nombre":"Jesús", "apellido":"Navas", "fecha_nacimiento":ISODate("1990-05-25"), "valor":40000000
    },
    {
        "id_integrante":04, "id_equipo":01, "tipo":"jugador","nombre":"Jules", "apellido":"Kounde", "fecha_nacimiento":ISODate("1998-11-12"), "valor":40000000
    },
    {
        "id_integrante":05, "id_equipo":02, "tipo":"entrenador","nombre":"Jose", "apellido":"Pelegrini", "fecha_nacimiento":ISODate("1970-02-06"), "valor":17000000
    },
    {
        "id_integrante":06, "id_equipo":02, "tipo":"medico","nombre":"Jose", "apellido":"Pascual", "fecha_nacimiento":ISODate("1985-09-24"), "valor":25000
    },
    {
        "id_integrante":07, "id_equipo":02, "tipo":"jugador","nombre":"Fernando", "apellido":"Fekir", "fecha_nacimiento":ISODate("1990-12-12"), "valor":35000000
    },
    {
        "id_integrante":08, "id_equipo":02, "tipo":"jugador","nombre":"Julián", "apellido":"Guardado", "fecha_nacimiento":ISODate("1991-02-28"), "valor":28000000
    },
    {
        "id_integrante":09, "id_equipo":03, "tipo":"entrenador","nombre":"Luis", "apellido":"Belmabrouk", "fecha_nacimiento":ISODate("1968-01-20") , "valor":8000000
    },
    {
        "id_integrante":10, "id_equipo":03, "tipo":"medico","nombre":"Darío", "apellido":"Pérez", "fecha_nacimiento":ISODate("1981-03-17"), "valor":10000
    },
    {
        "id_integrante":11, "id_equipo":03, "tipo":"jugador","nombre":"Pablo", "apellido":"Cuello", "fecha_nacimiento":ISODate("1989-09-25"), "valor":3000000
    },
    {
        "id_integrante":12, "id_equipo":03, "tipo":"jugador","nombre":"Fernando", "apellido":"González", "fecha_nacimiento":ISODate("1987-10-07"), "valor":1000000
    },
    {
        "id_integrante":13, "id_equipo":04, "tipo":"entrenador","nombre":"John", "apellido":"Koeman", "fecha_nacimiento":ISODate("1972-11-05"), "valor":25000000
    },
    {
        "id_integrante":14, "id_equipo":04, "tipo":"medico","nombre":"Luis", "apellido":"Alcántara", "fecha_nacimiento":ISODate("1986-12-14"), "valor":30000
    },
    {
        "id_integrante":15, "id_equipo":04, "tipo":"jugador","nombre":"Lionel", "apellido":"Messi", "fecha_nacimiento":ISODate("1989-07-13"), "valor":100000000
    },
    {
        "id_integrante":16, "id_equipo":04, "tipo":"jugador","nombre":"Marc André", "apellido":"Ter Stegen", "fecha_nacimiento":ISODate("1994-06-19"), "valor":40000000
    },
    {
        "id_integrante":17, "id_equipo":05, "tipo":"entrenador","nombre":"Lucas", "apellido":"Simeone", "fecha_nacimiento":ISODate("1960-05-26"), "valor":30000000
    },
    {
        "id_integrante":18, "id_equipo":05, "tipo":"medico","nombre":"Antonio", "apellido":"Núñez", "fecha_nacimiento":ISODate("1979-11-25"), "valor":35000
    },
    {
        "id_integrante":19, "id_equipo":05, "tipo":"jugador","nombre":"Luis", "apellido":"Suárez", "fecha_nacimiento":ISODate("1989-12-30"), "valor":70000000
    },
    {
        "id_integrante":20, "id_equipo":05, "tipo":"jugador","nombre":"Jan", "apellido":"Oblak", "fecha_nacimiento":ISODate("1992-04-08"), "valor":45000000
    },
    {
        "id_integrante":21, "id_equipo":06, "tipo":"entrenador","nombre":"Luis", "apellido":"Pólvora", "fecha_nacimiento":ISODate("1971-03-23"), "valor":10000000
    },
    {
        "id_integrante":22, "id_equipo":06, "tipo":"medico","nombre":"Jaime", "apellido":"Burdéos", "fecha_nacimiento":ISODate("1981-01-25"), "valor":10000
    },
    {
        "id_integrante":23, "id_equipo":06, "tipo":"jugador","nombre":"Iago", "apellido":"Aspas", "fecha_nacimiento":ISODate("1996-02-21"), "valor":30000000
    },
    {
        "id_integrante":24, "id_equipo":06, "tipo":"jugador","nombre":"Manuel", "apellido":"Fernández", "fecha_nacimiento":ISODate("1997-08-15"), "valor":1000000
    },
    {
        "id_integrante":25, "id_equipo":07, "tipo":"entrenador","nombre":"Ibai", "apellido":"Llanos", "fecha_nacimiento":ISODate("1978-10-14"), "valor":20000000
    },
    {
        "id_integrante":26, "id_equipo":07, "tipo":"medico","nombre":"Raúl", "apellido":"Serto", "fecha_nacimiento":ISODate("1988-12-10"), "valor":20000
    },
    {
        "id_integrante":27, "id_equipo":07, "tipo":"jugador","nombre":"Iñaki", "apellido":"Williams", "fecha_nacimiento":ISODate("1991-08-20"), "valor":30000000
    },
    {
        "id_integrante":28, "id_equipo":07, "tipo":"jugador","nombre":"Iker", "apellido":"Muniaín", "fecha_nacimiento":ISODate("1990-09-01"), "valor":10000000
    },
    {
        "id_integrante":29, "id_equipo":08, "tipo":"entrenador","nombre":"Ashley", "apellido":"Jordan", "fecha_nacimiento":ISODate("1970-05-09"), "valor":20000000
    },
    {
        "id_integrante":30, "id_equipo":08, "tipo":"medico","nombre":"Pedro", "apellido":"Jiménez", "fecha_nacimiento":ISODate("1980-01-11"), "valor":15000
    },
    {
        "id_integrante":31, "id_equipo":08, "tipo":"jugador","nombre":"Gerard", "apellido":"Moreno", "fecha_nacimiento":ISODate("1990-01-19"), "valor":20000000
    },
    {
        "id_integrante":32, "id_equipo":08, "tipo":"jugador","nombre":"Carlos", "apellido":"Vela", "fecha_nacimiento":ISODate("1998-03-22"), "valor":12000000
    },
    {
        "id_integrante":33, "id_equipo":09, "tipo":"entrenador","nombre":"Benito", "apellido":"Conejo", "fecha_nacimiento":ISODate("1967-07-28"), "valor":21000000
    },
    {
        "id_integrante":34, "id_equipo":09, "tipo":"medico","nombre":"Jimeno", "apellido":"Líbano", "fecha_nacimiento":ISODate("1982-12-23"), "valor":8000
    },
    {
        "id_integrante":35, "id_equipo":09, "tipo":"jugador","nombre":"Carlos", "apellido":"Bacca", "fecha_nacimiento":ISODate("1992-10-17"), "valor":8000000
    },
    {
        "id_integrante":36, "id_equipo":09, "tipo":"jugador","nombre":"Samuel", "apellido":"Castillejo", "fecha_nacimiento":ISODate("1999-02-26"), "valor":10000000
    },
    {
        "id_integrante":37, "id_equipo":10, "tipo":"entrenador","nombre":"Ricardo", "apellido":"López", "fecha_nacimiento":ISODate("1968-01-14"), "valor":10000000
    },
    {
        "id_integrante":38, "id_equipo":10, "tipo":"medico","nombre":"Gabriel", "apellido":"Pernezco", "fecha_nacimiento":ISODate("1982-02-25"), "valor":13000
    },
    {
        "id_integrante":39, "id_equipo":10, "tipo":"jugador","nombre":"Jaime", "apellido":"Botella", "fecha_nacimiento":ISODate("1991-02-15"), "valor":6000000
    },
    {
        "id_integrante":40, "id_equipo":10, "tipo":"jugador","nombre":"Ignacio", "apellido":"Ostos", "fecha_nacimiento":ISODate("1990-05-14"), "valor":900000
    },
    {
        "id_integrante":41, "id_equipo":11, "tipo":"entrenador","nombre":"Antonio", "apellido":"Borrego", "fecha_nacimiento":ISODate("1963-08-26"), "valor":14000000
    },
    {
        "id_integrante":42, "id_equipo":11, "tipo":"medico","nombre":"Enrique", "apellido":"Denostra", "fecha_nacimiento":ISODate("1978-03-07"), "valor":110000
    },
    {
        "id_integrante":43, "id_equipo":11, "tipo":"jugador","nombre":"Enrique", "apellido":"Capa", "fecha_nacimiento":ISODate("1987-01-01"), "valor":16000000
    },
    {
        "id_integrante":44, "id_equipo":11, "tipo":"jugador","nombre":"Manuel", "apellido":"Perro", "fecha_nacimiento":ISODate("1994-09-05"), "valor":1500000
    },
    {
        "id_integrante":45, "id_equipo":12, "tipo":"entrenador","nombre":"Zinedine", "apellido":"Zidane", "fecha_nacimiento":ISODate("1966-08-16"), "valor":31000000
    },
    {
        "id_integrante":46, "id_equipo":12, "tipo":"medico","nombre":"Pablo", "apellido":"Hidalgo", "fecha_nacimiento":ISODate("1980-05-05"), "valor":30000
    },
    {
        "id_integrante":47, "id_equipo":12, "tipo":"jugador","nombre":"Edem", "apellido":"Hazard", "fecha_nacimiento":ISODate("1990-11-28"), "valor":55000000
    },
    {
        "id_integrante":48, "id_equipo":12, "tipo":"jugador","nombre":"Lucas", "apellido":"Vázquez", "fecha_nacimiento":ISODate("1991-03-12"), "valor":20000000
    },
    {
        "id_integrante":49, "id_equipo":13, "tipo":"entrenador","nombre":"Eladio", "apellido":"Carro", "fecha_nacimiento":ISODate("1976-11-03"), "valor":11000000
    },
    {
        "id_integrante":50, "id_equipo":13, "tipo":"medico","nombre":"Jairo", "apellido":"Roldán", "fecha_nacimiento":ISODate("1984-10-09"), "valor":17000
    },
    {
        "id_integrante":51, "id_equipo":13, "tipo":"jugador","nombre":"Eustaquio", "apellido":"Lopera", "fecha_nacimiento":ISODate("1999-04-17"), "valor":7000000
    },
    {
        "id_integrante":52, "id_equipo":13, "tipo":"jugador","nombre":"Gabriel", "apellido":"Quiñónez", "fecha_nacimiento":ISODate("1993-05-17"), "valor":2000000
    },
    {
        "id_integrante":53, "id_equipo":14, "tipo":"entrenador","nombre":"Fabricio", "apellido":"Estrella", "fecha_nacimiento":ISODate("1977-01-02"), "valor":9000000
    },
    {
        "id_integrante":54, "id_equipo":14, "tipo":"medico","nombre":"Thiago", "apellido":"Rollo", "fecha_nacimiento":ISODate("1985-01-01"), "valor":13000
    },
    {
        "id_integrante":55, "id_equipo":14, "tipo":"jugador","nombre":"Paul", "apellido":"Lebron", "fecha_nacimiento":ISODate("1995-03-26"), "valor":1500000
    },
    {
        "id_integrante":56, "id_equipo":14, "tipo":"jugador","nombre":"Jose", "apellido":"Asistón", "fecha_nacimiento":ISODate("2001-12-25"), "valor":2000000
    },
    {
        "id_integrante":57, "id_equipo":15, "tipo":"entrenador","nombre":"Victor", "apellido":"Lupin", "fecha_nacimiento":ISODate("1972-09-07"), "valor":9900000
    },
    {
        "id_integrante":58, "id_equipo":15, "tipo":"medico","nombre":"Iago", "apellido":"Cabrales", "fecha_nacimiento":ISODate("1986-03-22"), "valor":20000
    },
    {
        "id_integrante":59, "id_equipo":15, "tipo":"jugador","nombre":"Laureano", "apellido":"Brazales", "fecha_nacimiento":ISODate("1994-01-30"), "valor":4200000
    },
    {
        "id_integrante":60, "id_equipo":15, "tipo":"jugador","nombre":"Jesús", "apellido":"Tornillo", "fecha_nacimiento":ISODate("1994-09-11"), "valor":2180000
    },
    {
        "id_integrante":61, "id_equipo":16, "tipo":"entrenador","nombre":"Ernesto", "apellido":"Barbacoa", "fecha_nacimiento":ISODate("1974-12-31"), "valor":20000000
    },
    {
        "id_integrante":62, "id_equipo":16, "tipo":"medico","nombre":"Luis", "apellido":"Rodríguez", "fecha_nacimiento":ISODate("1981-01-04"), "valor":10000
    },
    {
        "id_integrante":63, "id_equipo":16, "tipo":"jugador","nombre":"Felipe", "apellido":"Anderson", "fecha_nacimiento":ISODate("1989-05-25"), "valor":8360000
    },
    {
        "id_integrante":64, "id_equipo":16, "tipo":"jugador","nombre":"Raúl", "apellido":"Soria", "fecha_nacimiento":ISODate("1993-10-24"), "valor":9000000
    },
    {
        "id_integrante":65, "id_equipo":17, "tipo":"entrenador","nombre":"Pedro", "apellido":"Perrico", "fecha_nacimiento":ISODate("1977-02-12"), "valor":9000000
    },
    {
        "id_integrante":66, "id_equipo":17, "tipo":"medico","nombre":"Jairo", "apellido":"Tobal", "fecha_nacimiento":ISODate("1982-08-17"), "valor":7500
    },
    {
        "id_integrante":67, "id_equipo":17, "tipo":"jugador","nombre":"Aristo", "apellido":"Crasta", "fecha_nacimiento":ISODate("1996-07-16"), "valor":6000000
    },
    {
        "id_integrante":68, "id_equipo":17, "tipo":"jugador","nombre":"Julio", "apellido":"Fiesta", "fecha_nacimiento":ISODate("1996-12-28"), "valor":8000000
    },
    {
        "id_integrante":69, "id_equipo":18, "tipo":"entrenador","nombre":"Marcos", "apellido":"Jiménez", "fecha_nacimiento":ISODate("1970-12-04"), "valor":12000000
    },
    {
        "id_integrante":70, "id_equipo":18, "tipo":"medico","nombre":"Lukas", "apellido":"Caipironha", "fecha_nacimiento":ISODate("1980-08-06"), "valor":20000
    },
    {
        "id_integrante":71, "id_equipo":18, "tipo":"jugador","nombre":"Andrés", "apellido":"Inieso", "fecha_nacimiento":ISODate("1999-08-20"), "valor":750000
    },
    {
        "id_integrante":72, "id_equipo":18, "tipo":"jugador","nombre":"Hugo", "apellido":"Valderrama", "fecha_nacimiento":ISODate("1997-07-27"), "valor":225000
    },
    {
        "id_integrante":73, "id_equipo":19, "tipo":"entrenador","nombre":"Diego", "apellido":"Valkur", "fecha_nacimiento":ISODate("1976-08-25"), "valor":5000000
    },
    {
        "id_integrante":74, "id_equipo":19, "tipo":"medico","nombre":"Jaime", "apellido":"Correa", "fecha_nacimiento":ISODate("1974-05-21"), "valor":12000
    },
    {
        "id_integrante":75, "id_equipo":19, "tipo":"jugador","nombre":"Francisco", "apellido":"Soldado", "fecha_nacimiento":ISODate("1991-01-10"), "valor":11200000
    },
    {
        "id_integrante":76, "id_equipo":19, "tipo":"jugador","nombre":"Ivan", "apellido":"Teidoso", "fecha_nacimiento":ISODate("1990-11-11"), "valor":3000000
    },
    {
        "id_integrante":77, "id_equipo":20, "tipo":"entrenador","nombre":"Ever", "apellido":"Goslera", "fecha_nacimiento":ISODate("1969-02-20"), "valor":7630000
    },
    {
        "id_integrante":78, "id_equipo":20, "tipo":"medico","nombre":"Berto", "apellido":"Tomillo", "fecha_nacimiento":ISODate("1987-03-25"), "valor":11000
    },
    {
        "id_integrante":79, "id_equipo":20, "tipo":"jugador","nombre":"Álvaro", "apellido":"Negredo", "fecha_nacimiento":ISODate("1988-09-09"), "valor":12000000
    },
    {
        "id_integrante":80, "id_equipo":20, "tipo":"jugador","nombre":"Felipe", "apellido":"Blanco", "fecha_nacimiento":ISODate("1997-11-13"), "valor":4000000
    },
])
//En la siguiente colección se verá reconocido el integrante que será fichado y el equipo que ficha a dicho integrante.
db.traspasos.drop()
db.traspasos.insertMany([
    {
        "id_integrante":12,
        "id_equipo":14
    },
    {
        "id_integrante":21, "id_equipo":02
    },
    {
        "id_integrante":36, "id_equipo":19
    },
    {
        "id_integrante":48, "id_equipo":01
    },
    {
        "id_integrante":60, "id_equipo":10
    },
])