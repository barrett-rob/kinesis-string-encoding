const AWS = require('aws-sdk')
const https = require('https')

let kinesisParams = {
    apiVersion: '2013-12-02',
    httpOptions: {
        agent: new https.Agent({ rejectUnauthorized: false})
    }
}
if (process.env.ENDPOINT_URL) {
    kinesisParams['endpoint'] = new AWS.Endpoint(process.env.ENDPOINT_URL)
}
const kinesis = new AWS.Kinesis(kinesisParams)

let describeStreamParams = {
    StreamName: 'reservation-stream'
};
kinesis.describeStream(describeStreamParams, function (err, data) {
    if (err) {
        console.log(err, err.stack);
    } else {
        let shardId = data.StreamDescription.Shards[0].ShardId
        let getShardIteratorParams = {
            StreamName: 'reservation-stream',
            ShardId: shardId,
            ShardIteratorType: 'TRIM_HORIZON'
        };
        kinesis.getShardIterator(getShardIteratorParams, function (err, data) {
            if (err) {
                console.log(err, err.stack);
            } else {
                let shardIterator = data.ShardIterator
                let getRecordsParams = {
                    ShardIterator: shardIterator
                };
                kinesis.getRecords(getRecordsParams, function (err, data) {
                    if (err) {
                        console.log(err, err.stack);
                    } else {
                        data.Records.forEach(function (record, index) {
                            let recordString = record.Data.toString('utf8')
                            let parsedMessage = tryParse(recordString)
                            // if recordString has \u... escape sequences then parsed message will have unicode chars again
                            console.log(" -> (", index, ") retrieved string:", recordString, "parsed:", parsedMessage)
                        })
                    }
                });
            }
        });
    }
});

function tryParse(string) {
    try {
        return JSON.parse(string)
    } catch (err) {
        return string
    }
}