var AWS = require('aws-sdk');
var elasticsearch = require('elasticsearch');
var config = require('./config');

var esClient = new elasticsearch.Client({
	host: config.host
});

var s3 = new AWS.S3({
	accessKeyId: config.accessKeyId,
	secretAccessKey: config.secretAccessKey
});

function esIndex(data, callback) {
	var id = data.libraryID + '/' + data.key;
	data.key = undefined;
	
	var params = {
		index: config.index,
		type: config.type,
		id: id,
		routing: id,
		body: data
	};
	esClient.index(params, function (err, res) {
		callback(err);
	});
}

function esDelete(id, callback) {
	var params = {
		index: config.index,
		type: config.type,
		id: id,
		routing: id
	};
	
	esClient.delete(params, function (err, res) {
		callback(err);
	});
}

exports.handler = function (event, context) {
	var eventName = event.Records[0].eventName;
	var bucket = event.Records[0].s3.bucket.name;
	var key = event.Records[0].s3.object.key;
	
	if (/^ObjectCreated/.test(eventName)) {
		s3.getObject({Bucket: bucket, Key: key}, function (err, data) {
			if (err) return context.fail("Error getting file: " + err);
			
			var json = JSON.parse(data.Body.toString());
			esIndex(json, function (err) {
				if (err) return context.fail("Error indexing item: " + err);
				
				context.succeed();
			});
		});
	} else if (/^ObjectRemoved/.test(eventName)) {
		esDelete(key, function (err) {
			if (err) return context.fail("Error deleting item: " + err);
			
			context.succeed();
		});
	}
};
