const mysql = require("mysql");
const util = require("util");
const dxUtils = require("dx-utilities");

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
     * @returns {Promise<boolean>}
     */
    async init() {
        const dbConnectionSuccess = await this.checkDBConnection();

        if (!dbConnectionSuccess) {
            this.printLastError();
        }

        return dbConnectionSuccess;
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
            this.populateError("Invalid module name provided");
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
            this.populateError("Could not interact with the database", error);
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
            this.populateError("Could not connect to database", this.getLastError());
            return null;
        }

        try {
            await database.beginTransaction();
        } catch (error) {
            this.populateError("Error beginning", error);

            await database.close();
            return null;
        }

        return database;
    }

    /**
     * Commits a transaction to the database
     * @param {*} transaction The transaction object, which is basically just a connection to the database
     * @param {boolean} closeTransaction If set to false, the connection is not released after the commit
     */
    async commitTransaction(transaction = null, closeTransaction = true) {
        if (transaction === null) {
            this.populateError("Could not commit transaction. Invalid connection provided");
            return false;
        }

        let commitSuccess = true;
        try {
            commitSuccess = await transaction.commit();
        } catch (error) {
            commitSuccess = false;
            this.populateError("Error committing transaction", error);

            try {
                await transaction.rollback();
            } catch (error) {
                closeTransaction = true;
                this.populateError("Error rolling transaction back", error);
            }
        }

        if (!closeTransaction) {
            return commitSuccess;
        }

        try {
            commitSuccess &&= await this.closeTransaction(transaction);
        } catch (error) {
            this.populateError("Could not close transaction", error);
            commitSuccess = false;
        }

        return commitSuccess;
    }

    /**
     * Rolls back a transaction
     * @param {*} transaction The transaction object, which is basically just a connection to the database
     * @param {boolean} closeTransaction If set to false, the connection is not released after the rollback
     */
    async rollBackTransaction(transaction = null, closeTransaction = true) {
        if (transaction === null) {
            this.populateError("Could not roll back transaction. Invalid connection provided");
            return false;
        }

        let rollBackSuccess = true;
        try {
            rollBackSuccess = await transaction.rollback();
        } catch (error) {
            rollBackSuccess = false;
            closeTransaction = true;
            this.populateError("Error rolling transaction back", error);
        }

        if (!closeTransaction) {
            return rollBackSuccess;
        }

        try {
            rollBackSuccess &&= await this.closeTransaction(transaction);
        } catch (error) {
            this.populateError("Could not close transaction", error);
            rollBackSuccess = false;
        }

        return rollBackSuccess;
    }

    /**
     * Closes a transaction
     * @param {*} transaction The transaction object, which is basically just a connection to the database
     */
    async closeTransaction(transaction = null) {
        if (transaction === null) {
            this.populateError("Could not close transaction. Invalid connection provided");
            return false;
        }

        try {
            await transaction.close();
        } catch (error) {
            this.populateError("Could not close transaction", error);
            return false;
        }

        return true;
    }

    /**
     * Executes a single query on the configured database, based on the provided module name
     * @param {string|{sql: string, nestTables: string|boolean}} query The query to execute. Can also pass an options object as per nodejs-mysql
     * @param {string} moduleName The name of the module, corresponding to the module defined in dxconfig.json
     * @param {[]} values Any values to insert into placeholders in sql. If not provided, it is assumed that the query can execute as is
     * @param {{}} transaction An optional transaction object that contains the database connection that must be used for the query
     * @returns {Promise<{}|null>} Returns null when an error occurs. Call getError() for more information
     */
    async queryDB(query, moduleName, values, transaction) {
        if (typeof query === "undefined") {
            this.populateError("Invalid query provided");
            return null;
        }
        if (typeof moduleName === "undefined") {
            this.populateError("Invalid module name provided");
            return null;
        }

        const withTransaction = transaction !== undefined && transaction !== null;

        const database = withTransaction ? transaction : await this.connectDB(moduleName);

        if (database === null) {
            return null;
        }

        let queryResult = null;

        try {
            queryResult = await database.query(query, values);
        } catch (error) {
            queryResult = null;
            this.populateError("Could not query the database", error);
        }

        if (!withTransaction) {
            try {
                await database.close();
            } catch (error) {
                this.populateError("Could not close the database", error);
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

        let queryResult = null;
        try {
            queryResult = await this.queryWithTransaction(database, async () => {
                let queuedQueryResults = [];
                for (const query of queryArray) {
                    queuedQueryResults.push(await database.query(query.sql, query.values));
                }

                return queuedQueryResults;
            });
        } catch (error) {
            this.populateError("Error occurred during multi-query", error);
            queryResult = null;
        }

        return queryResult;
    }

    /**
     * Allows for executing a group of queries with potential rollback support
     * @param {*} database The local database instance
     * @param {function} callback The function called on completion
     * @returns {Promise<*|null>} Returns null when an error occurs. Call getLastError() for more information
     */
    async queryWithTransaction(database, callback) {
        if (database === null) {
            this.populateError("Tried to call queryWithTransaction, but database was NULL");
            return null;
        }

        let queryResult = null;
        try {
            await database.beginTransaction();
            queryResult = await callback();
            await database.commit();
        } catch (error) {
            queryResult = null;
            this.populateError("Could not query with transaction", error);

            try {
                await database.rollback();
            } catch (error) {
                this.populateError("Could not roll back transaction", error);
            }
        }

        await database.close();

        return queryResult;
    }

    /**
     * Simply checks whether we can connect to the relevant database for each defined module
     * @returns {Promise<boolean>}
     */
    async checkDBConnection() {
        for (const moduleName of this.moduleArray) {
            let moduleCheckSuccess = true;
            try {
                const database = await this.connectDB(moduleName);
                if (database === null) {
                    this.populateError("Error connecting to database", this.getLastError());
                }
                await database.close();
            } catch (error) {
                moduleCheckSuccess = false;
                this.populateError("Error connecting to database", error);
                await database.close();
            }

            if (!moduleCheckSuccess) {
                return false;
            }
        }

        return true;
    }

    //#region Error handling
    /**
     * Whenever Divblox encounters an error, the errorInfo array should be populated with details about the error. This
     * function simply returns that errorInfo array for debugging purposes
     * @returns {[]}
     */
    getError() {
        return this.errorInfo;
    }

    /**
     * Returns the latest error that was pushed, as an error object
     * @returns {{error: {}|null}} The latest error
     */
    getLastError() {
        let lastError = null;

        if (this.errorInfo.length > 0) {
            lastError = this.errorInfo[this.errorInfo.length - 1];
        }

        return lastError;
    }

    printLastError() {
        console.dir(this.getLastError(), { depth: null });
    }

    /**
     * Pushes a new error object/string into the error array
     * @param {{}|string} errorToPush An object, array or string containing error information
     * @param {{}|null} errorStack An object, containing error information
     * @param {boolean} mustClean If true, the errorInfo array will first be emptied before adding the new error.
     */
    populateError(errorToPush = "", errorStack = null, mustClean = false) {
        if (mustClean) {
            this.errorInfo = [];
        }

        if (!errorStack) {
            errorStack = errorToPush;
        }

        let message = "No message provided";
        if (typeof errorToPush === "string") {
            message = errorToPush;
        } else if (dxUtils.isValidObject(errorToPush)) {
            message = errorToPush.message ? errorToPush.message : "No message provided";
        } else {
            this.populateError("Invalid error type provided, errors can be only of type string or Object");
            return;
        }

        // Only the latest error to be of type DxBaseError
        let newErrorStack = {
            callerClass: errorStack.callerClass ? errorStack.callerClass : this.constructor.name,
            message: message ? message : errorStack.message ? errorStack.message : "No message provided",
            errorStack: errorStack.errorStack
                ? errorStack.errorStack
                : typeof errorStack === "string"
                ? null
                : errorStack,
        };

        const error = new DxBaseError(message, this.constructor.name, newErrorStack);

        // Make sure to keep the deepest stackTrace
        if (errorStack instanceof DxBaseError) {
            error.stack = errorStack.stack;
        }

        this.errorInfo.push(error);
        return;
    }

    /**
     * Resets the error info array
     */
    resetError() {
        this.errorInfo = [];
    }

    //#endregion
}

class DxBaseError extends Error {
    constructor(message = "", callerClass = "", errorStack = null, ...params) {
        // Pass remaining arguments (including vendor specific ones) to parent constructor
        super(...params);

        // Maintains proper stack trace for where our error was thrown (only available on V8)
        if (Error.captureStackTrace) {
            Error.captureStackTrace(this, DxBaseError);
        }

        this.name = "DxBaseError";

        // Custom debugging information
        this.message = message;
        this.callerClass = callerClass;
        this.dateTimeOccurred = new Date();
        this.errorStack = errorStack;
    }
}

module.exports = DivbloxDatabaseConnector;
