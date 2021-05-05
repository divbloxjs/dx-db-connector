const mysql = require('mysql');
const util = require('util');

/**
 * Responsible for connecting to the configured database and execute queries
 */
class DivbloxDatabaseConnector {
    /**
     * Takes the config array (example of which can be seen in test.js) and sets up the relevant connection information
     * for later use
     * @param databaseConfigArray This is defined in the dxconfig.json file
     */
    constructor(databaseConfigArray = {}) {
        this.databaseConfig = {};
        this.errorInfo = [];
        this.moduleArray = Object.keys(databaseConfigArray);
        for (const moduleName of this.moduleArray) {
            this.databaseConfig[moduleName] = databaseConfigArray[moduleName];
        }
        this.isInitComplete = false;
    }

    /**
     * Does all the required work to ensure that database communication is working correctly before continuing
     * @returns {Promise<void>}
     */
    async init() {
        try {
            await this.checkDBConnection();
            this.isInitComplete = true;
        } catch (error) {
            this.errorInfo.push("Error checking db connection: "+error);
        }
    }

    /**
     * Validates whether the init function managed to complete and pushes an error message to the error array if not
     * @returns {boolean} true if validated, false if not
     */
    validateInitComplete() {
        if (!this.isInitComplete) {
            this.errorInfo.push("Database connector init not completed. Cannot execute query. " +
                "Please run init() after instantiating the database connector");
        }
        return this.isInitComplete;
    }

    /**
     * Whenever Divblox encounters an error, the errorInfo array is populated with details about the error. This
     * function simply returns that errorInfo array for debugging purposes
     * @returns {[]}
     */
    getError() {
        return this.errorInfo;
    }

    /**
     * Connect to a configured database, based on the provided module name
     * @param moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {null|{rollback(): any, beginTransaction(): any, query(*=, *=): any, commit(): any, close(): any}|*}
     */
    connectDB(moduleName = null) {
        if (moduleName === null) {
            this.errorInfo.push("Invalid module name NULL provided");
            return null;
        }
        try {
            const connection = mysql.createConnection(this.databaseConfig[moduleName]);
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
            this.errorInfo.push(error);
            return null;
        }

    }

    /**
     * Executes a single query on the configured database, based on the provided module name
     * @param query The query to execute
     * @param moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {Promise<{}|null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryDB(query = null,moduleName = null) {
        if (query === null) {
            this.errorInfo.push("Invalid query NULL provided");
        }
        if (!this.validateInitComplete()) {
            return null;
        }
        const database = this.connectDB(moduleName);
        if (database === null) {
            return null;
        }
        let queryResult = {};
        try {
            queryResult = await database.query(query);
        } catch (error) {
            // handle the error
            queryResult = {"error":error};
        } finally {
            try {
                await database.close();
            } catch (error) {
                queryResult = {"error":error};
            }
        }
        return queryResult;
    }

    /**
     * A wrapper for queryDB which takes an array of queries to execute
     * @param queryArray The array of queries to execute
     * @param moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {Promise<{}|null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryDBMultiple(queryArray = [], moduleName = null) {
        if (!this.validateInitComplete()) {
            return null;
        }
        const database = this.connectDB(moduleName);
        if (database === null) {
            return null;
        }
        let queryResult = {};
        try {
            await queryWithTransaction(database, async () => {
                let tempData = [];
                for (const query of queryArray) {
                    tempData.push(await database.query(query));
                }
                queryResult = tempData;
            } );
        } catch (error) {
            // handle error
            queryResult = {"error":error};
        }
        return queryResult;
    }

    /**
     * Allows for executing a group of queries with potential rollback support
     * @param database The local database instance
     * @param callback The function called on completion
     * @returns {Promise<null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryWithTransaction(database, callback) {
        if (database === null) {
            this.errorInfo.push("Tried to call queryWithTransaction, but database was NULL");
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
        for (const moduleName of this.moduleArray) {
            try {
                const database = this.connectDB(moduleName);
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


