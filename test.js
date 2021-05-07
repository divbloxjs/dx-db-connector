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

    console.log("Inserting into table 'test' with placeholders");
    let queryResult = await databaseConnector.queryDB(
        "INSERT INTO `test` (`column1`, `column2`) VALUES (?, ?);",
        "main",[333,"Test string"]);

    if (queryResult === null) {
        console.error("Error while querying: "+JSON.stringify(databaseConnector.getError(),null,2));
    } else {
        console.dir(queryResult);
    }

    console.log("Inserting into table 'test' without placeholders");
    queryResult = await databaseConnector.queryDB(
        "INSERT INTO `test` (`column1`, `column2`) VALUES (999, 'Testing string without placeholder');",
        "main",[]);

    if (queryResult === null) {
        console.error("Error while querying: "+JSON.stringify(databaseConnector.getError(),null,2));
    } else {
        console.dir(queryResult);
    }

    console.log("Querying * from table 'test'");
    queryResult = await databaseConnector.queryDB("SELECT * FROM `test`",
        "main");

    if (queryResult === null) {
        console.error("Error while querying: "+JSON.stringify(databaseConnector.getError(),null,2));
    } else {
        console.dir(queryResult);
    }
}
doTest();