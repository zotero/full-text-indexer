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

import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { Client as ESClient, errors as esErrors } from '@elastic/elasticsearch';
import config from 'config';
import zlib from 'zlib';

const es = new ESClient({
	node: config.get('es.host'),
	requestTimeout: 5000
});

const s3Client = new S3Client();

async function esIndex(data) {
	let id = data.libraryID + '/' + data.key;
	
	// Key is not needed
	delete data.key;
	
	console.log(`Indexing ${id}`);
	
	try {
		await es.index({
			index: config.get('es.index'),
			id: id,
			version: data.version,
			version_type: 'external_gt',
			routing: data.libraryID,
			body: data
		});
	}
	catch (e) {
		// Ignore version conflict
		if (e instanceof esErrors.ResponseError && e.statusCode == 409) {
			console.log('Version conflict');
		} else {
			throw e;
		}
	}
}

async function esDelete(libraryID, key) {
	let id = libraryID + '/' + key;
	
	console.log(`Deleting ${id}`);
	
	try {
		await es.delete({
			index: config.get('es.index'),
			id: id,
			routing: libraryID,
		});
	}
	catch (e) {
		// Ignore delete if missing from Elasticsearch
		if (e instanceof esErrors.ResponseError && e.statusCode == 404) {
			console.log('Not found');
		} else {
			throw e;
		}
	}
}

async function processEvent(event) {
	// Always gets only one event per invocation
	let eventName = event.Records[0].eventName;
	let bucket = event.Records[0].s3.bucket.name;
	let key = event.Records[0].s3.object.key;
	let eTagEvent = event.Records[0].s3.object.eTag;
	
	if (/^ObjectCreated/.test(eventName)) {
		let data;
		try {
			let command = new GetObjectCommand({Bucket: bucket, Key: key});
			data = await s3Client.send(command);
		}
		catch (e) {
			// This generally shouldn't happen, but it can if we're processing the DLQ after
			// an extended outage and the item has already been deleted
			if (e.name == "NoSuchKey") {
				console.log(`${key} not found`);
				return;
			}
			throw e;
		}
		
		// S3 returns eTag wrapped in quotes
		let eTagObject = data.ETag.slice(1, -1);
		
		if (eTagEvent !== eTagObject) {
			throw new Error(`Event eTag differs from S3 object eTag for ${key} (${eTagEvent} != ${eTagObject}`);
		}
		
		let json = JSON.parse(
			data.ContentType === 'application/gzip'
				? zlib.unzipSync(await data.Body.transformToByteArray())
				: await data.Body.transformToString()
		);
		
		await esIndex(json);
	}
	else if (/^ObjectRemoved/.test(eventName)) {
		let parts = key.split('/');
		await esDelete(parts[0], parts[1]);
	}
}

export const s3 = async function (event) {
	await processEvent(event);
};

export const dlq = async function (event, context) {
	let sqs = new SQSClient();
	let lambda = new LambdaClient();
	
	let queueURL = config.get('sqsURL');
	let params;
	let numProcessed = 0;
	try {
		while (context.getRemainingTimeInMillis() > 10000) {
			params = {
				QueueUrl: queueURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout: 10,
			};
			let command = new ReceiveMessageCommand(params);
			let data = await sqs.send(command);
			
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
			command = new InvokeCommand(params);
			let result = await lambda.send(command);
			if (result.FunctionError) {
				let payload = Buffer.from(result.Payload).toString();
				
				// Continue on ETag mismatch, since full text just might have changed since this
				// event was queued
				let { errorMessage } = JSON.parse(payload);
				if (errorMessage && errorMessage.includes('Event eTag differs from S3 object eTag')) {
					console.warn(errorMessage);
					continue;
				}
				
				console.warn(payload);
				return;
			}
			
			params = {
				QueueUrl: queueURL,
				ReceiptHandle: message.ReceiptHandle,
			};
			command = new DeleteMessageCommand(params);
			await sqs.send(command);
			numProcessed++;
		}
	}
	catch (err) {
		console.error(err);
	}
	finally {
		console.log(`Processed ${numProcessed} message${numProcessed == 1 ? '' : 's'}`);
	}
};
