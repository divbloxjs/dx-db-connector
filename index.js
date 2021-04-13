const mysql = require('mysql');
const util = require('util');

/**
 * Responsible for connecting to the configured database and execute queries
 */
class DivbloxDatabaseConnector {
    /**
     * Takes the config array (example of which can be seen in test.js) and sets up the relevant connection information
     * for later use
     * @param database_config_array This is defined in the dxconfig.json file
     */
    constructor(database_config_array = {}) {
        this.database_config = {};
        this.error_info = [];
        this.module_array = Object.keys(database_config_array);
        for (const module_name_str of this.module_array) {
            this.database_config[module_name_str] = database_config_array[module_name_str];
        }
        this.is_init_complete = false;
    }

    /**
     * Does all the required work to ensure that database communication is working correctly before continuing
     * @returns {Promise<void>}
     */
    async init() {
        try {
            await this.checkDBConnection();
            this.is_init_complete = true;
        } catch (error) {
            this.error_info.push("Error checking db connection: "+error);
        }
    }

    /**
     * Validates whether the init function managed to complete and pushes an error message to the error array if not
     * @returns {boolean} true if validated, false if not
     */
    validateInitComplete() {
        if (!this.is_init_complete) {
            this.error_info.push("Database connector init not completed. Cannot execute query. " +
                "Please run init() after instantiating the database connector");
        }
        return this.is_init_complete;
    }

    /**
     * Whenever Divblox encounters an error, the error_info array is populated with details about the error. This
     * function simply returns that error_info array for debugging purposes
     * @returns {[]}
     */
    getError() {
        return this.error_info;
    }

    /**
     * Connect to a configured database, based on the provided module name
     * @param module_name_str The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {null|{rollback(): any, beginTransaction(): any, query(*=, *=): any, commit(): any, close(): any}|*}
     */
    connectDB(module_name_str = null) {
        if (module_name_str === null) {
            this.error_info.push("Invalid module name NULL provided");
            return null;
        }
        try {
            const connection = mysql.createConnection(this.database_config[module_name_str]);
            return {
                query( sql, args ) {
                    return util.promisify(connection.query)
                        .call(connection, sql, args);
                },
                beginTransaction() {
                    return util.promisify(connection.beginTransaction)
                        .call(connection);
                },
                commit() {
                    return util.promisify(connection.commit)
                        .call(connection);
                },
                rollback() {
                    return util.promisify(connection.rollback)
                        .call(connection);
                },
                close() {
                    return util.promisify(connection.end).call(connection);
                }
            };
        } catch (error) {
            this.error_info.push(error);
            return null;
        }

    }

    /**
     * Executes a single query on the configured database, based on the provided module name
     * @param query_str The query to execute
     * @param module_name_str The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {Promise<{}|null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryDB(query_str = null,module_name_str = null) {
        if (query_str === null) {
            this.error_info.push("Invalid query_str NULL provided");
        }
        if (!this.validateInitComplete()) {
            return null;
        }
        const database = this.connectDB(module_name_str);
        if (database === null) {
            return null;
        }
        let query_result = {};
        try {
            query_result = await database.query(query_str);
        } catch (error) {
            // handle the error
            query_result = {"error":error};
        } finally {
            try {
                await database.close();
            } catch (error) {
                query_result = {"error":error};
            }
        }
        return query_result;
    }

    /**
     * A wrapper for queryDB which takes an array of queries to execute
     * @param query_strings_arr The array of queries to execute
     * @param module_name_str The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {Promise<{}|null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryDBMultiple(query_strings_arr = [], module_name_str = null) {
        if (!this.validateInitComplete()) {
            return null;
        }
        const database = this.connectDB(module_name_str);
        if (database === null) {
            return null;
        }
        let query_result = {};
        try {
            await queryWithTransaction(database, async () => {
                let temp_data = [];
                for (const query_str of query_strings_arr) {
                    temp_data.push(await database.query(query_str));
                }
                query_result = temp_data;
            } );
        } catch (error) {
            // handle error
            query_result = {"error":error};
        }
        return query_result;
    }

    /**
     * Allows for executing a group of queries with potential rollback support
     * @param database The local database instance
     * @param callback The function called on completion
     * @returns {Promise<null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryWithTransaction(database, callback) {
        if (database === null) {
            this.error_info.push("Tried to call queryWithTransaction, but database was NULL");
            return null;
        }
        try {
            await database.beginTransaction();
            await callback();
            await database.commit();
        } catch (error) {
            await database.rollback();
            throw error;
        } finally {
            await database.close();
        }
    }

    /**
     * Simply checks whether we can connect to the relevant database for each defined module
     * @returns {Promise<boolean>}
     */
    async checkDBConnection() {
        for (const module_name_str of this.module_array) {
            try {
                const database = this.connectDB(module_name_str);
                if (database === null) {
                    throw new Error("Error connecting to database: "+JSON.stringify(this.getError(),null,2));
                }
                await database.close();
            } catch (error) {
                throw new Error("Error connecting to database: "+error);
            }
        }
        return true;
    }
}

module.exports = DivbloxDatabaseConnector;


