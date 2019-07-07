/**
 * The Sql Model class
 *
 * @constructor
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {Object}    options
 */
var Sql = Function.inherits('Alchemy.Model', function Sql(options) {
	Sql.super.call(this, options);
});

/**
 * This is a wrapper class
 */
Sql.makeAbstractClass();

/**
 * Don't add default fields
 *
 * @author   Jelle De Loecker <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
Sql.setProperty('add_basic_fields', false);

/**
 * Set the name of the default primary key field
 *
 * @author   Jelle De Loecker <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
Sql.setProperty('primary_key', 'id');

/**
 * Constitute the class wide schema
 *
 * @author   Jelle De Loecker <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
Sql.constitute(function addProjectFields() {
	this.schema.remove('_id');
	this.schema.remove('created');
	this.schema.remove('updated');
});