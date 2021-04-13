const db = require("./index");
const db_config_default = {
    "environment_array":{
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
    const database_connector = new db(db_config_default["environment_array"]["development"]["modules"]);
    await database_connector.init();
    console.log("Querying * from table 'test'");
    const query_result = await database_connector.queryDB("SELECT * FROM `test`","main");
    if (query_result === null) {
        console.error("Error while querying: "+JSON.stringify(database_connector.getError(),null,2));
    } else {
        console.dir(query_result);
    }
}
doTest();