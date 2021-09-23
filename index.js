const mongo = require('mongodb');
const mongoClient = mongo.MongoClient;

class DatabaseError extends Error {
	constructor (message) {
		super(message);
	}
}

class Database {
	openConnection(name, url, timeout) {
		const self = this;
		this.name = name;
		this.url = url;

		console.error("Trying to connect to server: " + this.url);
		mongoClient.connect(this.url + this.name, {useNewUrlParser: true, useUnifiedTopology: true},
			function(err, connection) {
				if (err) {
					console.error(err);
					setTimeout(() => {
						self.openConnection(name, url, timeout);
					}, timeout);
				} else {
					self.setConnection(connection);
				}
			}
		);
	}

	closeConnection() {
		console.error("Closing down connection with: " + this.name + "...");
		if (this.connection != undefined) {
			this.connection.close();
			console.error("Connection successfully closed.");
			return;
		}
		console.error("Couldn't close connection with: " + this.name + "...");
		return;
	}

	setConnection(connection) {
		if (connection == undefined) {
			console.error("Connection isn't alive.");
			return 84;
		}
		this.connection = connection;
		this.database = connection.db(this.name);
		console.error("Using database: " + this.name);
		return 0;
	}

	checkConnection() {
		if (this.database == undefined || this.connection == undefined) {
			return false;
		}
		return true;
	}

	setDatabase(connection) {
		if (connection == undefined) {
			console.error("Couldn't assign database to handler.");
			return;
		}
		this.connection = connection;
		this.database = connection.db(this.name);
		console.error("Database successfully assigned to handler.");
		console.error("Successfully connected to: " + this.name + '.');
		return;
	}

	checkRequestValidity(parameters) {
		for (let param of parameters) {
			if (param == undefined) {
				return false;
			}
		}
		return this.checkConnection();
	}

	getCollection(collectionName) {
		return this.database.collection(collectionName);
	}

	executeRequest(collectionName, parameters, callback) {
		if (!this.checkRequestValidity(parameters)) {
			return Promise.reject(new DatabaseError("Mongo request is invalid."));
		}
		let collection = this.getCollection(collectionName);
		if (collection == undefined) {
			return Promise.reject(new DatabaseError("Collection doesn't exists."));
		}
		return callback(collection, parameters);
	}

	insertInCollection(collectionName, data) {
		return this.executeRequest(collectionName, Array.isArray(data) ? data : [data], function(collection, parameters) {
			return collection.insertOne(parameters[0]);
		});
	}

	insertManyInCollection(collectionName, data) {
		return this.executeRequest(collectionName, [data], function (collection, parameters) {
			return collection.insertMany(parameters[0]);
		});
	}

	distinctInCollection(collectionName, field, query) {
		return this.executeRequest(collectionName, [field, query], function(collection, parameters) {
			return collection.distinct(parameters[0], parameters[1]);
		});
	}

	findOneInCollection(collectionName, query) {
		return this.executeRequest(collectionName, Array.isArray(query) ? query : [query], function(collection, parameters) {
			return collection.findOne(parameters[0]);
		});
	}

	findInCollection(collectionName, query) {
		return this.executeRequest(collectionName, Array.isArray(query) ? query : [query], function(collection, parameters) {
			return collection.find(parameters[0]).toArray();
		});
	}

	findSortInCollection(collectionName, query, sort) {
		return this.executeRequest(collectionName, [query, sort], function(collection, parameters) {
			return collection.find(parameters[0]).sort(parameters[1]).toArray();
		});
	}

	removeOneInCollection(collectionName, query) {
		return this.executeRequest(collectionName, Array.isArray(query) ? query : [query], function(collection, parameters) {
			return collection.deleteOne(parameters[0]);
		});
	}

	removeInCollection(collectionName, query) {
		return this.executeRequest(collectionName, Array.isArray(query) ? query : [query], function(collection, parameters) {
			return collection.deleteMany(parameters[0]);
		});
	}

	updateInCollection(collectionName, query, data) {
		return this.executeRequest(collectionName, [query, data], function(collection, parameters) {
			return collection.updateMany(parameters[0], parameters[1]);
		});
	}

	updateOneInCollection(collectionName, query, data) {
		return this.executeRequest(collectionName, [query, data], function(collection, parameters) {
			return collection.updateOne(parameters[0], parameters[1]);
		});
	}

	findAndUpdateOneInCollection(collectionName, query, data) {
		return this.executeRequest(collectionName, [query, data], function(collection, parameters) {
			return collection.findOneAndUpdate(parameters[0], parameters[1]);
		});
	}

	findSortSkipLimitInCollection(collectionName, query, sort, skip, limit) {
		return this.executeRequest(collectionName, [query, sort, skip, limit], function(collection, parameters) {
			return collection.find(parameters[0]).sort(parameters[1]).skip(parameters[2]).limit(parameters[3]).toArray();
		});
	}
}

let database = new Database();

module.exports = database;