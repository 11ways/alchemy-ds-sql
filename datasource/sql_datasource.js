const ID_GLOBAL_REGEXP = /`/g,
      libpath = alchemy.use('path'),
      Mosql = alchemy.use('mongo-sql'),
      bson = alchemy.use('bson');

/**
 * Base SQL Datasource
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
var SqlDS = Function.inherits('Alchemy.Datasource.Nosql', function Sql(name, options) {
	Sql.super.call(this, name, options);
});

// Indicate this datasource does NOT support objectids
SqlDS.setSupport('objectid', false);

// Indicate this datasource does NOT querying associations for now
SqlDS.setSupport('querying_associations', false);

/**
 * Perform a CREATE TABLE query
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {String}   table_name
 *
 * @return   {Pledge}
 */
SqlDS.setMethod(function createTable(table_name) {
	var sql = 'CREATE TABLE IF NOT EXISTS ' + this.escapeName(table_name) + ' (_id VARCHAR(24) PRIMARY KEY)';
	return this.queryCommand(sql);
});

/**
 * Escape a name
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {String}   name
 *
 * @return   {String}
 */
SqlDS.setMethod(function escapeName(name) {
	return '`' + String(name).replace(ID_GLOBAL_REGEXP, '``') + '`';
});

/**
 * Load external schema for the given Model
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {Function}   ModelClass
 *
 * @return   {Pledge}
 */
SqlDS.setMethod(function loadExternalSchema(ModelClass) {

	var that = this,
	    proto = ModelClass.prototype,
	    table = ModelClass.table,
	    safe_table = this.escapeName(table);

	let pledge = Function.series(function getTableInfo(next) {
		that.getTableInfo(table).done(next);
	}, function addSchemaFields(next, info) {

		var field_name,
		    config,
		    type;

		for (field_name in info) {
			config = info[field_name];
			type = null;

			switch (config.type.toLowerCase()) {
				case 'integer':
					type = 'Number';
					break;

				case 'text':
					type = 'Text';
					break;

				default:
					console.log('Unknown field type:', config);
			}

			if (type) {
				ModelClass.addField(field_name, type);
			}
		}

	}, null);

	return pledge;
});

/**
 * Setup the datasource
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {Function}   ModelClass
 *
 * @return   {Pledge}
 */
SqlDS.setMethod(function configureTable(ModelClass) {

	var that = this,
	    proto = ModelClass.prototype,
	    table = ModelClass.table;

	if (proto.load_external_schema) {
		return that.loadExternalSchema(ModelClass);
	}

	let safe_table = this.escapeName(table);

	let pledge = Function.series(function createTable(next) {
		that.createTable(table).done(next);
	}, function getTableInfo(next) {
		that.getTableInfo(table).done(next);
	}, function createFields(next, info) {

		var tasks = [];

		ModelClass.schema.forEach(function each(value, key) {

			// @TODO: allow switching from one datatype to another
			if (info[key]) {
				return;
			}

			let safe_name = that.escapeName(key),
			    type,
			    sql;

			switch (value.datatype) {
				case 'string':
					type = 'TEXT';
					break;

				case 'objectid':
					//type = 'VARCHAR(24)';
					type = 'TEXT';
					break;

				case 'boolean':
					type = 'TINYINT(1)';
					break;

				case 'number':
					type = 'INT';
					break;

				case 'date':
					type = 'DATE';
					break;

				case 'time':
				case 'datetime':
					type = 'DATETIME';
					break;

				case 'object':
					type = 'TEXT';
					break;

				default:
					return next(new Error('Unknown datatype: ' + value.datatype));
			}

			sql = 'ALTER TABLE ' + safe_table + ' ADD COLUMN ' + safe_name + ' ' + type;

			tasks.push(function addColumn(next) {
				that.queryCommand(sql).done(next);
			});
		});

		let sub_pledge = Function.series(tasks, next);

	}, null);

	return pledge;
});

/**
 * Create a record in the database
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
SqlDS.setMethod(function _create(model, data, options, callback) {

	var that = this,
	    unzipped = Object.unzip(data),
	    sql;

	sql = 'INSERT INTO ' + this.escapeName(model.table) + ' ';
	sql += '(' + unzipped.keys.map(this.escapeName).join(', ') + ') ';
	sql += 'VALUES (' + unzipped.keys.map(function(val, index) {return '?';}) + ')';

	this.queryCommand(sql.toString(), unzipped.values).done(function finished(err, result) {

		if (err) {
			return callback(err);
		}

		// Clear the cache
		model.nukeCache();

		if (Array.isArray(result)) {
			result = result[0];
		}

		// Some databases (like MySQL) return the inserted record,
		// others (like SQLite3) do not
		if (result && result._id) {
			return callback(null, result);
		}

		that.queryAll('SELECT * FROM ' + that.escapeName(model.table) + ' WHERE _id = ?', [data._id]).done(function queried(err, rows) {

			if (err) {
				return callback(err);
			}

			callback(null, rows[0]);
		});
	});
});

/**
 * Query the database
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
SqlDS.setMethod(async function _read(model, criteria, callback) {

	var that = this,
	    sql;

	let sql_config,
	    compiled,
	    options;

	await criteria.normalize();

	compiled = await that.compileCriteria(criteria);

	sql_config = {
		type  : 'select',
		table : model.table,
		where : compiled
	};

	options = that.compileCriteriaOptions(criteria);

	if (options.limit) {
		sql_config.limit = options.limit;
	}

	if (options.skip) {
		sql_config.offset = options.skip;
	}

	if (options.sort) {
		let new_sort = {},
		    key;

		if (Array.isArray(options.sort)) {
			let entry,
			    i;

			for (i = 0; i < options.sort.length; i++) {
				entry = options.sort[i];
				new_sort[entry[0]] = entry[1] == 1 ? 'asc' : 'desc';
			}
		} else {
			for (key in options.sort) {
				new_sort[key] = options.sort[key] == 1 ? 'asc' : 'desc';
			}
		}

		sql_config.order = new_sort;
	}

	sql = Mosql.sql(sql_config);

	this.queryAll(sql.toString(), sql.values).done(function finished(err, result) {

		if (err) {
			return callback(err);
		}

		return callback(null, result, null);
	});

});
