const mysql = require("mysql");
const util = require("util");

/**
 * Responsible for connecting to the configured database and execute queries
 */
class DivbloxDatabaseConnector {
    /**
     * Takes the config array (example of which can be seen in test.js) and sets up the relevant connection information
     * for later use
     * @param {{}} databaseConfig The database configuration object for each module. Each module is a separate
     * database. An example is shown below:
     * "mainModule": {
                    "host": "localhost",
                    "user": "dbuser",
                    "password": "123",
                    "database": "local_dx_db",
                    "port": 3306,
                    "ssl": false
                },
     "secondaryModule": {
                    "host": "localhost",
                    "user": "dbuser",
                    "password": "123",
                    "database": "local_dx_db",
                    "port": 3306,
                    "ssl": {
                        ca: "Contents of __dirname + '/certs/ca.pem'",
                        key: "Contents of __dirname + '/certs/client-key.pem'",
                        cert: "Contents of __dirname + '/certs/client-cert.pem'"
                    }
                },
     */
    constructor(databaseConfig = {}) {
        this.databaseConfig = {};
        this.connectionPools = {};
        this.errorInfo = [];
        this.moduleArray = Object.keys(databaseConfig);
        for (const moduleName of this.moduleArray) {
            this.databaseConfig[moduleName] = databaseConfig[moduleName];
            this.connectionPools[moduleName] = mysql.createPool(databaseConfig[moduleName]);
        }
    }

    /**
     * Does all the required work to ensure that database communication is working correctly before continuing
     * @returns {Promise<void>}
     */
    async init() {
        try {
            await this.checkDBConnection();
        } catch (error) {
            this.errorInfo.push("Error checking db connection: " + error);
        }
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
     * Returns a connection from the connection pool
     * @param {string} moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @return {Promise<*>}
     */
    async getPoolConnection(moduleName) {
        return util.promisify(this.connectionPools[moduleName].getConnection).call(this.connectionPools[moduleName]);
    }

    /**
     * Connect to a configured database, based on the provided module name
     * @param {string} moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {null|{rollback(): any, beginTransaction(): any, query(*=, *=): any, commit(): any, close(): any}|*}
     */
    async connectDB(moduleName) {
        if (typeof moduleName === "undefined") {
            this.errorInfo.push("Invalid module name provided");
            return null;
        }
        try {
            const connection = await this.getPoolConnection(moduleName);
            return {
                query(sql, args) {
                    return util.promisify(connection.query).call(connection, sql, args);
                },
                beginTransaction() {
                    return util.promisify(connection.beginTransaction).call(connection);
                },
                commit() {
                    return util.promisify(connection.commit).call(connection);
                },
                rollback() {
                    return util.promisify(connection.rollback).call(connection);
                },
                close() {
                    return connection.release();
                },
            };
        } catch (error) {
            this.errorInfo.push(error);
            return null;
        }
    }

    /**
     * Starts a new transaction on the database and returns the database connection
     * @param {string} moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {Promise<{}|null>} Returns null if a database transaction could not be started
     */
    async beginTransaction(moduleName) {
        const database = await this.connectDB(moduleName);

        if (database === null) {
            this.errorInfo.push("Could not connect to database");
            return null;
        }

        try {
            await database.beginTransaction();
        } catch (error) {
            await database.close();

            this.errorInfo.push("Error connecting to database: " + error);

            return null;
        }

        return database;
    }

    /**
     * Commits a transaction to the database
     * @param {*} transaction The transaction object, which is basically just a connection to the database
     */
    async commitTransaction(transaction = null) {
        if (transaction === null) {
            this.errorInfo.push("Could not commit transaction. Invalid connection provided");
            return false;
        }

        try {
            await transaction.commit();
        } catch (error) {
            await transaction.rollback();

            this.errorInfo.push("Error committing transaction: " + error);

            return false;
        }
        return true;
    }

    /**
     * Rolls back a transaction
     * @param {*} transaction The transaction object, which is basically just a connection to the database
     */
    async rollBackTransaction(transaction = null) {
        if (transaction === null) {
            this.errorInfo.push("Could not roll back transaction. Invalid connection provided");
            return false;
        }

        await transaction.rollback();
        return true;
    }

    /**
     * Closes a transaction
     * @param {*} transaction The transaction object, which is basically just a connection to the database
     */
    async closeTransaction(transaction = null) {
        if (transaction === null) {
            this.errorInfo.push("Could not close transaction. Invalid connection provided");
            return false;
        }

        await transaction.close();
        return true;
    }

    /**
     * Executes a single query on the configured database, based on the provided module name
     * @param {string} query The query to execute
     * @param {string} moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @param {[]} values Any values to insert into placeholders in sql. If not provided, it is assumed that the query
     * @param {{}} transaction An optional transaction object that contains the database connection that must be used for the query
     * can execute as is
     * @returns {Promise<{}|null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryDB(query, moduleName, values, transaction) {
        if (typeof query === "undefined") {
            this.errorInfo.push("Invalid query provided");
        }
        if (typeof moduleName === "undefined") {
            this.errorInfo.push("Invalid module name provided");
        }
        const withTransaction = transaction !== undefined && transaction !== null;

        const database = withTransaction ? transaction : await this.connectDB(moduleName);

        if (database === null) {
            return null;
        }

        let queryResult = {};

        try {
            queryResult = await database.query(query, values);
        } catch (error) {
            // handle the error
            queryResult = { error: error };
        } finally {
            if (!withTransaction) {
                try {
                    database.close();
                } catch (error) {
                    queryResult = { error: error };
                }
            }
        }
        return queryResult;
    }

    /**
     * A wrapper for queryDB which takes an array of queries to execute
     * @param {[{sql:string,values:[]}]} queryArray The array of queries to execute. Each query is an object
     * containing the sql and possible placeholder values to process. If values is not provided, it is assumed that the
     * query can execute as is
     * @param {string} moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @returns {Promise<{}|null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryDBMultiple(queryArray = [], moduleName = null) {
        const database = await this.connectDB(moduleName);
        if (database === null) {
            return null;
        }
        let queryResult = {};
        try {
            await queryWithTransaction(database, async () => {
                let tempData = [];
                for (const query of queryArray) {
                    tempData.push(await database.query(query.sql, query.values));
                }
                queryResult = tempData;
            });
        } catch (error) {
            queryResult = { error: error };
        }
        return queryResult;
    }

    /**
     * Allows for executing a group of queries with potential rollback support
     * @param {*} database The local database instance
     * @param {function} callback The function called on completion
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
            database.close();
        }
    }

    /**
     * Simply checks whether we can connect to the relevant database for each defined module
     * @returns {Promise<boolean>}
     */
    async checkDBConnection() {
        for (const moduleName of this.moduleArray) {
            try {
                const database = await this.connectDB(moduleName);
                if (database === null) {
                    throw new Error("Error connecting to database: " + JSON.stringify(this.getError(), null, 2));
                }
                database.close();
            } catch (error) {
                throw new Error("Error connecting to database: " + error);
            }
        }
        return true;
    }
}

module.exports = DivbloxDatabaseConnector;
