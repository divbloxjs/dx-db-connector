const db = require("./index");
const dbConfigDefault = {
    "environmentArray":{
        "development":{
            "modules": {
                "main": {
                    "host": "localhost",
                    "user": "dbuser",
                    "password": "123",
                    "database": "local_db",
                    "port": 3306,
                    "ssl": false
                }
            }
        },
        "production":{
            "modules": {
                "main": {
                    "host": "localhost",
                    "user": "dbuser",
                    "password": "123",
                    "database": "local_db",
                    "port": 3306,
                    "ssl": false
                }
            }
        }
    }
};
async function doTest() {
    const databaseConnector = new db(dbConfigDefault["environmentArray"]["development"]["modules"]);
    await databaseConnector.init();
    console.log("Querying * from table 'test'");
    const queryResult = await databaseConnector.queryDB("SELECT * FROM `test`","main");
    if (queryResult === null) {
        console.error("Error while querying: "+JSON.stringify(databaseConnector.getError(),null,2));
    } else {
        console.dir(queryResult);
    }
}
doTest();