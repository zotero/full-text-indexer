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
const config = require('./config');

const SQS = new AWS.SQS({ apiVersion: '2012-11-05' });
const Lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });

const es = new elasticsearch.Client({
	host: config.es.host
});

const s3 = new AWS.S3(config.s3);

async function esIndex(data) {
	let id = data.libraryID + '/' + data.key;
	
	// Key is not needed
	data.key = undefined;
	
	let params = {
		index: config.es.index,
		type: config.es.type,
		id: id,
		// From ES 2.0 'routing' must be provided with a query, not mapping
		routing: data.libraryID,
		body: data
	};
	
	await es.index(params);
}

async function esDelete(libraryID, key) {
	let id = libraryID + '/' + key;
	
	let params = {
		index: config.es.index,
		type: config.es.type,
		id: id,
		// From ES 2.0 'routing' must be provided with a query, not mapping
		routing: libraryID,
	};
	
	await es.delete(params);
}

async function processEvent(event) {
	let eventName = event.Records[0].eventName;
	let bucket = event.Records[0].s3.bucket.name;
	let key = event.Records[0].s3.object.key;
	
	if (/^ObjectCreated/.test(eventName)) {
		let data = await s3.getObject({Bucket: bucket, Key: key}).promise();
		let json = JSON.parse(data.Body.toString());
		await esIndex(json);
	}
	else if (/^ObjectRemoved/.test(eventName)) {
		let parts = key.split('/');
		await esDelete(parts[0], parts[1]);
	}
}

exports.handler = async function (event, context) {
	let params;
	try {
		params = {
			QueueUrl: config.sqsUrl,
			MaxNumberOfMessages: 1,
			VisibilityTimeout: 10,
		};
		let data = await SQS.receiveMessage(params).promise();

		if (!data || !data.Messages || !data.Messages.length) return;
		
		let message = data.Messages[0];
		
		await processEvent(JSON.parse(message.Body));
		
		params = {
			QueueUrl: config.sqsUrl,
			ReceiptHandle: message.ReceiptHandle,
		};
		await SQS.deleteMessage(params).promise();
	} catch(err) {
		console.log(err);
		return;
	}
	
	params = {
		FunctionName: context.functionName,
		InvocationType: 'Event',
		Payload: JSON.stringify(event),
		Qualifier: context.functionVersion
	};

	Lambda.invoke(params);
};
