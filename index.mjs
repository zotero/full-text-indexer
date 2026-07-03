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
	paginateListObjectsV2
} from "@aws-sdk/client-s3";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import { DynamoDBClient, GetItemCommand, PutItemCommand, UpdateItemCommand, DeleteItemCommand } from "@aws-sdk/client-dynamodb";
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

const ddbClient = new DynamoDBClient();

// Number of S3 keys to list (and checkpoint) per reindex loop iteration. AWS caps MaxKeys at 1000.
const REINDEX_S3_BATCH_SIZE = 100;

// Per-library full-text state lives in a shared DynamoDB table (single-table design: partition
// key `pk` is an entity-typed key like `LIBRARY#<id>`, sort key `sk` names the record). The
// library's state item (sk `STATE`) carries a `deindexed` flag; a missing item or absent/false
// flag means the library is not deindexed (the common case) and is indexed normally. A library is
// "deindexed" when its content has been removed from Elasticsearch (e.g., purged because the
// owner doesn't use the web library). The flag is maintained mainly by an external
// purge/reconciliation script; dataserver clears it (deindexed=false) when a library is queued
// for reindexing. We skip indexing new content while deindexed; the content stays in S3, so a
// later reindex restores everything.
async function isLibraryDeindexed(libraryID) {
	let resp = await ddbClient.send(new GetItemCommand({
		TableName: config.get('dynamoTable'),
		Key: { pk: { S: `LIBRARY#${libraryID}` }, sk: { S: 'STATE' } },
		ProjectionExpression: 'deindexed'
	}));
	return resp.Item?.deindexed?.BOOL === true;
}

async function esIndex(data, reindex = false) {
	let id = data.libraryID + '/' + data.key;

	// Key is not needed
	delete data.key;

	console.log(`Indexing ${id}`);

	try {
		await es.index({
			index: config.get('es.index'),
			id: id,
			version: data.version,
			// Live indexing uses external_gt to drop stale/out-of-order events. A reindex is an
			// authoritative refill, so it uses external_gte: it re-applies the stored content (and
			// restores a just-deleted doc at the same version) but still yields to a newer live
			// version, so a concurrent upload is never clobbered.
			version_type: reindex ? 'external_gte' : 'external_gt',
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

async function processEvent(event, reindex = false) {
	// Always gets only one event per invocation
	let eventName = event.Records[0].eventName;
	let bucket = event.Records[0].s3.bucket.name;
	let key = event.Records[0].s3.object.key;
	let eTagEvent = event.Records[0].s3.object.eTag;
	
	if (/^ObjectCreated/.test(eventName)) {
		// Skip libraries that have been removed from the index (content stays in S3 for
		// later reindexing). The key is "<libraryID>/<itemKey>".
		let libraryID = key.split('/')[0];
		if (await isLibraryDeindexed(libraryID)) {
			console.log(`Library ${libraryID} is deindexed; skipping ${key}`);
			return;
		}

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
		
		await esIndex(json, reindex);
	}
	else if (/^ObjectRemoved/.test(eventName)) {
		let parts = key.split('/');
		await esDelete(parts[0], parts[1]);
	}
}

export const s3 = async function (event) {
	await processEvent(event);
};

export const indexDLQMessage = async function (event) {
	await processEvent(event);
};

export const dlq = async function (event, context) {
	let sqs = new SQSClient();
	let lambda = new LambdaClient();
	
	let queueURL = config.get('dlqURL');
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
				FunctionName: config.get('dlqIndexerFunctionName'),
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
	const reindexStateKey = { pk: { S: `LIBRARY#${libraryID}` }, sk: { S: 'REINDEX' } };
	let reindexStatus = {};
	// Running count of objects enqueued (the indexable total), carried across resume cycles and
	// recorded on the STATE item at the end so the API can tell "fully indexed" from "rebuilding".
	let enqueued = 0;
	// Resume from a prior unfinished run if a checkpoint exists in the state table
	let reindexStateResp = await ddbClient.send(new GetItemCommand({
		TableName: config.get('dynamoTable'),
		Key: reindexStateKey
	}));
	if (reindexStateResp.Item) {
		reindexStatus.lastKey = reindexStateResp.Item.lastKey?.S;
		if (reindexStateResp.Item.count?.N) {
			enqueued = Number(reindexStateResp.Item.count.N);
		}
		console.log("Reindex checkpoint found", reindexStatus);
	}
	else {
		console.log("No reindex checkpoint");
	}

	let sqs = new SQSClient();
	// Paginate with the SDK helper so continuation is handled internally. Reusing a single
	// ListObjectsV2Command and mutating ContinuationToken paginates unreliably (overlapping and
	// skipped pages), which double-enqueues some keys and drops others.
	const pages = paginateListObjectsV2(
		{ client: s3Client, pageSize: REINDEX_S3_BATCH_SIZE },
		{
			Bucket: config.get('s3Bucket'),
			Prefix: `${libraryID}/`,
			// Resume from the last checkpointed key if a prior run didn't finish
			...(reindexStatus.lastKey ? { StartAfter: reindexStatus.lastKey } : {})
		}
	);
	let forceStop = false;
	console.log(`Reindexing library ${libraryID} starting from key: ${reindexStatus.lastKey || "-"}`);
	for await (const page of pages) {
		// Stop if there's a chance of timeout; the checkpoint lets the next invocation resume
		if (context.getRemainingTimeInMillis() < 6000) {
			forceStop = true;
			break;
		}
		const Contents = page.Contents ?? [];
		if (!Contents.length) {
			continue;
		}
		enqueued += Contents.length;

		// Enqueue each item as a synthetic S3 event on the reindex index queue, where the
		// consumer indexes it the same way as a live S3 event
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

		let sqsSendEventPromises = [];
		// Group fake S3 events in batches of 10 (current max for SQS send batch command) and send to SQS
		while (sqsEvents.length > 0) {
			let batch = sqsEvents.splice(0, 10);
			const command = new SendMessageBatchCommand({
				QueueUrl: config.get('reindexIndexQueueURL'),
				Entries: batch
			});
			sqsSendEventPromises.push(sqs.send(command));
		}
		// Wait for all batches to be added
		await Promise.all(sqsSendEventPromises);

		// Record the last added key, so the next invocation knows where to resume
		let lastKey = Contents[Contents.length - 1].Key;
		reindexStatus.lastKey = lastKey;

		// Save the checkpoint for the next lambda run, if it's needed
		await ddbClient.send(new PutItemCommand({
			TableName: config.get('dynamoTable'),
			Item: { pk: { S: `LIBRARY#${libraryID}` }, sk: { S: 'REINDEX' }, lastKey: { S: lastKey }, count: { N: String(enqueued) } }
		}));
	}

	if (forceStop) {
		console.log("Forced stop");
		// If we timed out, return the key back to the queue
		return {
			batchItemFailures: [{
				itemIdentifier: event.Records[0].messageId
			}]
		};
	}
	// Refill done: record the indexable total (objects enqueued) and clear the `reindexing`
	// flag.  indexableCount lets the API report "indexed" once ES matches what's actually in
	// S3, even if stored DB rows exceed that for some reason (e.g., failed S3 writes). REMOVE
	// drops `reindexing`; UpdateItem so `deindexed`/other attrs survive.
	await ddbClient.send(new UpdateItemCommand({
		TableName: config.get('dynamoTable'),
		Key: { pk: { S: `LIBRARY#${libraryID}` }, sk: { S: 'STATE' } },
		UpdateExpression: 'SET indexableCount = :n REMOVE reindexing',
		ExpressionAttributeValues: { ':n': { N: String(enqueued) } }
	}));

	// Delete the reindex checkpoint
	await ddbClient.send(new DeleteItemCommand({
		TableName: config.get('dynamoTable'),
		Key: reindexStateKey
	}));
};

// SQS event-source consumer for the reindex index queue. Each record's body is a synthetic S3
// event produced by reindexLibrary; index it via the shared processEvent path. Per-record
// failures are reported so only they get retried.
export const reindexIndex = async function (event) {
	const batchItemFailures = [];
	for (const record of event.Records) {
		try {
			// reindex=true: authoritative refill (external_gte) — restores stored content but
			// won't overwrite a newer live version
			await processEvent(JSON.parse(record.body), true);
		}
		catch (e) {
			// The object changed since reindexLibrary listed it, so the live S3-event pipeline has
			// already (re)indexed it; treat as done rather than retry the stale event forever.
			if (e.message && e.message.includes('Event eTag differs from S3 object eTag')) {
				console.warn(e.message);
				continue;
			}
			console.error(e);
			batchItemFailures.push({ itemIdentifier: record.messageId });
		}
	}
	return { batchItemFailures };
};
