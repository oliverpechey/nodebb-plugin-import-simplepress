var async = require('async');
var mysql = require('mysql');
var _ = require('lodash');
var extend = require('extend');
var noop = function() {};
var logPrefix = '[nodebb-plugin-import-simplepress]';
(function(Exporter) {
	Exporter.setup = function(config, callback) {
		Exporter.log('setup');
		// mysql db only config
		// extract them from the configs passed by the nodebb-plugin-import adapter
		var _config = {
			host: config.dbhost || config.host || 'localhost',
			user: config.dbuser || config.user || 'root',
			password: config.dbpass || config.pass || config.password || '',
			port: config.dbport || config.port || 3306,
			database: config.dbname || config.name || config.database,
		};
		Exporter.config(_config);
		Exporter.config('prefix', config.prefix || config.tablePrefix || '');
		config.custom = config.custom || {};
		if (typeof config.custom === 'string') {
			try {
				config.custom = JSON.parse(config.custom)
			} catch (e) {}
		}
		config.custom = config.custom || {};
		config.custom.timemachine = config.custom.timemachine || {};
		config.custom = extend(true, {}, {
			timemachine: {
				messages: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				},
				users: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				},
				topics: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				},
				categories: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				},
				posts: {
					from: config.custom.timemachine.from || null,
					to: config.custom.timemachine.to || null
				}
			}
		}, config.custom);
		Exporter.config('custom', config.custom);
		Exporter.connection = mysql.createConnection(_config);
		Exporter.connection.connect();
		setInterval(function() {
			Exporter.connection.query("SELECT 1", function() {});
		}, 60000);
		callback(null, Exporter.config());
	};

	Exporter.query = function(query, callback) {
		if (!Exporter.connection) {
			var err = {
				error: 'MySQL connection is not setup. Run setup(config) first'
			};
			Exporter.error(err.error);
			return callback(err);
		}
		console.log('\n\n====QUERY====\n\n' + query + '\n');
		Exporter.connection.query(query, function(err, rows) {
			if (rows) {
				console.log('returned: ' + rows.length + ' results');
			}
			callback(err, rows)
		});
	};

	Exporter.getUsers = function(callback) {
		return Exporter.getPaginatedUsers(0, -1, callback);
	};

	Exporter.getPaginatedUsers = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT ' +
			prefix + 'users.ID as _uid, ' +
			prefix + 'users.user_email as _email, ' +
			prefix + 'users.user_login as _username, ' +
			prefix + 'users.display_name as _alternativeUsername, ' +
			prefix + 'users.user_email as _email, ' +
			'UNIX_TIMESTAMP(' + prefix + 'users.user_registered) * 1000 as _joindate, ' +
			prefix + 'sfmembers.signature as _signature, ' +
			'UNIX_TIMESTAMP(' + prefix + 'sfmembers.lastvisit) * 1000 as _lastonline' +
			' FROM ' + prefix + 'users ' +
			' LEFT JOIN ' + prefix + 'sfmembers ON ' + prefix + 'sfmembers.user_id = ' + prefix + 'users.ID ' +
			' WHERE 1 = 1 ' +
			(start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');
		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				//normalize here
				var map = {};
				rows.forEach(function(row) {
					// lower case the email for consistency
					row._email = (row._email || '')
						.toLowerCase();
					map[row._uid] = row;
				});
				callback(null, map);
			});
	};

    Exporter.getRooms = function(callback) {
        return Exporter.getPaginatedRooms(0, -1, callback);
    };
    Exporter.getPaginatedRooms = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var query =
            'SELECT ' +
            't.thread_id as _roomId, ' +
            'r.user_id as _uid, ' +
            'm.user_id as _uids, ' +
            'UNIX_TIMESTAMP(sent_date) * 1000 as _timestamp' +
            'FROM ' + prefix + 'sfpmthreads t ' +
            'join ' + prefix + 'sfpmmessages m on m.message_id = (select m2.message_id from ' + prefix + 'sfpmmessages m2 where m2.thread_id = t.thread_id order by m2.message_id asc LIMIT 1) ' +
            'join ' + prefix + 'sfpmrecipients r on m.message_id = r.message_id and r.user_id != m.user_id ' +
            (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');
    
            Exporter.query(query,
                function(err, rows) {
                    if (err) {
                        Exporter.error(err);
                        return callback(err);
                    }
    
                    //normalize here
                    var map = {};
    
                    rows.forEach(function(row) {
                        map[row._roomId] = row;
                    });
    
                    callback(null, map);
                });
    };

	Exporter.getMessages = function(callback) {
		return Exporter.getPaginatedMessages(0, -1, callback);
	};

	Exporter.getPaginatedMessages = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix') || '';
		var query = 'SELECT ' +
			prefix + 'sfpmmessages.message_id as _mid, ' +
			prefix + 'sfpmmessages.user_id as _fromuid, ' +
			prefix + 'sfpmrecipients.user_id as _touid, ' +
			'UNIX_TIMESTAMP(' + prefix + 'sfpmmessages.message) * 1000 as _content, ' +
			prefix + 'sfpmmessages.sent_date as _timestamp ' +
			'FROM ' + prefix + 'sfpmmessages ' +
			'LEFT JOIN ' + prefix + 'sfpmrecipients ON ' + prefix + 'sfpmmessages.message_id = ' + prefix + 'sfpmrecipients.message_id' +
			' WHERE 1 = 1 ' +
			(start >= 0 && limit >= 0 ? ' LIMIT ' + start + ',' + limit : '');
		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				//normalize here
				var map = {};
				rows.forEach(function(row) {
					map[row._mid] = row;
				});
				callback(null, map);
			});
	};

	Exporter.getCategories = function(callback) {
		return Exporter.getPaginatedCategories(0, -1, callback);
	};

	Exporter.getPaginatedCategories = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');
		var query = 'SELECT ' +
			prefix + 'sfgroups.group_id as _cid, ' +
			'null as _parentCid, ' +
			prefix + 'sfgroups.group_name as _name, ' +
			prefix + 'sfgroups.group_seq as _order, ' +
			'null as _description ' +
			'FROM ' + prefix + 'sfgroups ' +
			'UNION SELECT ' +
			prefix + 'sfforums.forum_id + (select count(*) from wp_sfgroups) as _cid, ' +
			prefix + 'sfforums.group_id as _parentCid, ' +
			prefix + 'sfforums.forum_name as _name, ' +
			prefix + 'sfforums.forum_seq as _order, ' +
			prefix + 'sfforums.forum_desc as _description ' +
			'FROM ' + prefix + 'sfforums ' +
			(start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');
		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				//normalize here
				var map = {};
				rows.forEach(function(row) {
					map[row._cid] = row;
				});
				callback(null, map);
			});
	};

	Exporter.getTopics = function(callback) {
		return Exporter.getPaginatedTopics(0, -1, callback);
	};

	Exporter.getPaginatedTopics = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var err;
		var prefix = Exporter.config('prefix');
		var startms = +new Date();
		var query =
			'SELECT ' +
			prefix + 'sftopics.forum_id + (select count(*) from wp_sfgroups) as _cid, ' +
			prefix + 'sftopics.topic_id as _tid, ' +
			prefix + 'sftopics.user_id as _uid, ' +
			prefix + 'sftopics.topic_opened as _viewcount, ' +
			prefix + 'sftopics.topic_name as _title, ' +
			'UNIX_TIMESTAMP(' + prefix + 'sftopics.topic_date) * 1000 as _timestamp, ' +
			prefix + 'sftopics.topic_status as _locked, ' +
			prefix + 'sfposts.post_content as _content, ' +
			prefix + 'sfposts.poster_ip as _ip, ' +
			prefix + 'sfposts.guest_name as _guest ' +
			' FROM ' + prefix + 'sftopics' +
			' LEFT JOIN ' + prefix + 'sfposts ON ' + prefix + 'sftopics.topic_id = ' + prefix + 'sfposts.topic_id and ' +
			prefix + 'sfposts.post_index = 1 ' +
			(start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');
		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				//normalize here
				var map = {};
				rows.forEach(function(row, i) {
					row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
					map[row._tid] = row;
				});
				callback(null, map, rows);
			});
	};

	Exporter.getPosts = function(callback) {
		return Exporter.getPaginatedPosts(0, -1, callback);
	};

	Exporter.getPaginatedPosts = function(start, limit, callback) {
		callback = !_.isFunction(callback) ? noop : callback;
		var prefix = Exporter.config('prefix');
		var query =
			'SELECT ' +
			prefix + 'sfposts.forum_id + (select count(*) from wp_sfgroups) as _cid, ' +
			prefix + 'sfposts.topic_id as _tid, ' +
			prefix + 'sfposts.post_id as _pid, ' +
			'UNIX_TIMESTAMP(' + prefix + 'sfposts.post_date) * 1000 as _timestamp, ' +
			prefix + 'sfposts.post_content as _content, ' +
			prefix + 'sfposts.user_id as _uid, ' +
			prefix + 'sfposts.guest_name as _guest ' +
			'FROM ' + prefix + 'sfposts ' +
			'WHERE ' + prefix + 'sfposts.post_index > 1 ' +
			(start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');
		Exporter.query(query,
			function(err, rows) {
				if (err) {
					Exporter.error(err);
					return callback(err);
				}
				//normalize here
				var map = {};
				rows.forEach(function(row) {
					row._content = row._content || '';
					row._deleted = row._deleted ? 1 : 0
					row._edited = row._edited ? row._edited * 1000 : null

                    // It skips the post if the uid is 0. Use guest name instead.
                    if(row._uid == 0)
                        delete row._uid

					map[row._pid] = row;
				});
				callback(null, map);
			});
	};

	Exporter.teardown = function(callback) {
		Exporter.log('teardown');
		Exporter.connection.end();
		Exporter.log('Done');
		callback();
	};

	Exporter.testrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getUsers(next);
			},
            function(next) {
                Exporter.getRooms(next);
            },
			function(next) {
				Exporter.getMessages(next);
			},
			function(next) {
				Exporter.getCategories(next);
			},
			function(next) {
				Exporter.getTopics(next);
			},
			function(next) {
				Exporter.getPosts(next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.paginatedTestrun = function(config, callback) {
		async.series([
			function(next) {
				Exporter.setup(config, next);
			},
			function(next) {
				Exporter.getPaginatedUsers(0, 1000, next);
			},
            function(next) {
                Exporter.getPaginatedRooms(0,1000, next);
            },
			function(next) {
				Exporter.getPaginatedMessages(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedCategories(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedTopics(0, 1000, next);
			},
			function(next) {
				Exporter.getPaginatedPosts(1001, 2000, next);
			},
			function(next) {
				Exporter.teardown(next);
			}
		], callback);
	};

	Exporter.warn = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.warn.apply(console, args);
	};

	Exporter.log = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.log.apply(console, args);
	};

	Exporter.error = function() {
		var args = _.toArray(arguments);
		args.unshift(logPrefix);
		console.error.apply(console, args);
	};

	Exporter.config = function(config, val) {
		if (config != null) {
			if (typeof config === 'object') {
				Exporter._config = config;
			} else if (typeof config === 'string') {
				if (val != null) {
					Exporter._config = Exporter._config || {};
					Exporter._config[config] = val;
				}
				return Exporter._config[config];
			}
		}
		return Exporter._config;
	};

	// from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
	Exporter.validateUrl = function(url) {
		var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
		return url && url.length < 2083 && url.match(pattern) ? url : '';
	};

	Exporter.truncateStr = function(str, len) {
		if (typeof str != 'string') return str;
		len = _.isNumber(len) && len > 3 ? len : 20;
		return str.length <= len ? str : str.substr(0, len - 3) + '...';
	};

	Exporter.whichIsFalsy = function(arr) {
		for (var i = 0; i < arr.length; i++) {
			if (!arr[i])
				return i;
		}
		return null;
	};
})(module.exports);