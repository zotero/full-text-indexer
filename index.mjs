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

import { S3Client,
	GetObjectCommand,
	ListObjectsV2Command,
	PutObjectCommand,
	DeleteObjectCommand
} from "@aws-sdk/client-s3";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import {
	SQSClient,
	ReceiveMessageCommand,
	DeleteMessageCommand,
	SendMessageBatchCommand
} from "@aws-sdk/client-sqs";
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
	
	// Ignore events triggered by _reindex_status file
	if (key.includes("_reindex_status")) {
		return;
	}
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
		while (context.getRemainingTimeInMillis() > 6000) {
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

export const reindexLibrary = async (event, context) => {
	const body = JSON.parse(event.Records[0].body);
	const libraryID = body.libraryID;
	const reindexStatusKey = `${libraryID}/_reindex_status`;
	let reindexStatus = {};
	// Try to fetch reindex status file from s3. If there was an unfinished reindexing run,
	// it will exist.
	try {
		let reindexStatusFile = await s3Client.send(new GetObjectCommand({
			Bucket: config.get('s3Bucket'),
			Key: reindexStatusKey
		}));
		let reindexStatusBody = await reindexStatusFile.Body.transformToString();
		reindexStatus = JSON.parse(reindexStatusBody);
		console.log("Reindex status file found", reindexStatus);
	}
	catch (e) {
		console.log("No reindex status file");
		// Otherwise, this is a fresh run, so we add it.
		await s3Client.send(new PutObjectCommand({
			Bucket: config.get('s3Bucket'),
			Key: reindexStatusKey,
			Body: JSON.stringify(reindexStatus)
		}));
	}

	let sqs = new SQSClient();
	let listObjectsInput = {
		Bucket: config.get('s3Bucket'),
		Prefix: `${libraryID}/`,
		MaxKeys: config.get('s3BatchSize')
	};
	if (reindexStatus.lastKey) {
		// Start from the last processed key if this is not the first run
		listObjectsInput.StartAfter = reindexStatus.lastKey;
	}
	const listObjectsCommand = new ListObjectsV2Command(listObjectsInput);
	let forceStop = false;
	console.log(`Reindexing library ${libraryID} starting from key: ${reindexStatus.lastKey || "-"}`);
	// Keep fetching all items in s3 while we have the continuation token
	do {
		// Stop if there is a chance of timeout
		if (context.getRemainingTimeInMillis() < 6000) {
			forceStop = true;
			break;
		}
		var { Contents, IsTruncated, NextContinuationToken } = await s3Client.send(listObjectsCommand);

		// Create fake s3 events that will be added to DLQ as if previously
		// failed events for re-indexing.
		let sqsEvents = Contents.map((entry) => {
			const message = {
				Records:
					[{
						eventName: "ObjectCreated",
						s3: {
							bucket: { name: config.get('s3Bucket') },
							object: {
								key: entry.Key,
								eTag: entry.ETag.slice(1, -1)
							}
						},
					}]
			};
			return {
				Id: entry.Key.replace("/", "-"),
				MessageBody: JSON.stringify(message)
			};
		});

		listObjectsCommand.input.ContinuationToken = NextContinuationToken;

		let sqsSendEventPromises = [];
		// Group fake s3 events in batches of 10 (current max for SQS send batch command) and send to sqs
		while (sqsEvents.length > 0) {
			let batch = sqsEvents.splice(0, 10);
			const command = new SendMessageBatchCommand({
				QueueUrl: config.get('sqsURL'),
				Entries: batch
			});
			sqsSendEventPromises.push(sqs.send(command));
		}
		// Wait for all batches to be added
		await Promise.all(sqsSendEventPromises);
		
		// Record the last added key to the queue, so that the next lambda knows where
		// to start processing from
		let lastKey = Contents[Contents.length - 1].Key;
		reindexStatus.lastKey = lastKey;

		// Save reindexStatus to s3 for the next lambda run, if it's needed
		await s3Client.send(new PutObjectCommand({
			Bucket: config.get('s3Bucket'),
			Key: reindexStatusKey,
			Body: JSON.stringify(reindexStatus)
		}));
	} while (IsTruncated);

	if (forceStop) {
		console.log("Forced stop");
		// If we timed out, return the key back to the queue
		return {
			batchItemFailures: [{
				itemIdentifier: event.Records[0].messageId
			}]
		};
	}
	// Reindexing has been finished, delete the temp reindex status file
	await s3Client.send(new DeleteObjectCommand({
		Bucket: config.get('s3Bucket'),
		Key: reindexStatusKey
	}));
};
