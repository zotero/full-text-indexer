/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2018 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const AWS = require('aws-sdk');
const elasticsearch = require('elasticsearch');
const config = require('config');
const zlib = require('zlib');

const SQS = new AWS.SQS({apiVersion: '2012-11-05'});
const Lambda = new AWS.Lambda({apiVersion: '2015-03-31'});

const es = new elasticsearch.Client({
	host: config.get('es.host'),
	requestTimeout: 5000
});

const s3 = new AWS.S3();

async function esIndex(data) {
	let id = data.libraryID + '/' + data.key;
	
	// Key is not needed
	delete data.key;
	
	let params = {
		index: config.get('es.index'),
		type: config.get('es.type'),
		id: id,
		// From ES 2.0 'routing' must be provided with a query, not mapping
		routing: data.libraryID,
		body: data
	};
	
	console.log(`Indexing ${id}`);
	await es.index(params);
}

async function esDelete(libraryID, key) {
	let id = libraryID + '/' + key;
	
	let params = {
		index: config.get('es.index'),
		type: config.get('es.type'),
		id: id,
		// From ES 2.0 'routing' must be provided with a query, not mapping
		routing: libraryID,
	};
	
	console.log(`Deleting ${id}`);
	try {
		await es.delete(params);
	}
	catch (e) {
		// Ignore delete if missing from Elasticsearch
		if (e instanceof elasticsearch.errors.NotFound) {
			console.log("Not found");
			return;
		}
		throw e;
	}
}

async function processEvent(event) {
	// Always gets only one event per invocation
	let eventName = event.Records[0].eventName;
	let bucket = event.Records[0].s3.bucket.name;
	let key = event.Records[0].s3.object.key;
	
	if (/^ObjectCreated/.test(eventName)) {
		let data;
		try {
			data = await s3.getObject({Bucket: bucket, Key: key}).promise();
		}
		catch (e) {
			// This generally shouldn't happen, but it can if we're processing the DLQ after
			// an extended outage and the item has already been deleted
			if (e.statusCode == 404) {
				console.log(`${key} not found`);
				return;
			}
			throw e;
		}
		
		let json = data.Body;
		
		if(data.ContentType === 'application/gzip') {
			json = zlib.unzipSync(json);
		}
		
		json = JSON.parse(json.toString());
		
		await esIndex(json);
	}
	else if (/^ObjectRemoved/.test(eventName)) {
		let parts = key.split('/');
		await esDelete(parts[0], parts[1]);
	}
}

exports.s3 = async function (event) {
	await processEvent(event);
};

exports.dlq = async function (event, context) {
	let queueURL = config.get('sqsURL');
	let params;
	let numProcessed = 0;
	try {
		// Process one DLQ message per lambda invocation
		while (context.getRemainingTimeInMillis() > 10000) {
			params = {
				QueueUrl: queueURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout: 10,
			};
			let data = await SQS.receiveMessage(params).promise();
			
			if (!data || !data.Messages || !data.Messages.length) {
				console.log("No messages in queue");
				return;
			}
			
			let message = data.Messages[0];
			
			if (numProcessed % 10 == 0) {
				console.log(`Processed ${numProcessed} messages`);
			}
			params = {
				FunctionName: config.get('s3FunctionName'),
				InvocationType: 'RequestResponse',
				Payload: message.Body
			};
			let result = await Lambda.invoke(params).promise();
			if (result.FunctionError) {
				console.log(result);
				return;
			}
			
			params = {
				QueueUrl: queueURL,
				ReceiptHandle: message.ReceiptHandle,
			};
			await SQS.deleteMessage(params).promise();
			numProcessed++;
		}
	}
	catch (err) {
		console.log(err);
	}
	finally {
		console.log(`Processed ${numProcessed} message${numProcessed == 1 ? '' : 's'}`);
	}
};
