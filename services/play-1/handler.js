'use strict';
global.globalInt = 1;
global.globalstart = new Date().toTimeString()


module.exports.endpoint = (event, context, callback) => {
  global.globalInt = globalInt+1;
  const response = {
    statusCode: 200,
    body: JSON.stringify({
      message: `Hello, the current time is ${new Date().toTimeString()}. globalCount is ${globalInt} start: ${globalstart}`,
    }),
  };

  callback(null, response);
};

//const sleep = (waitTimeInMs) => new Promise(resolve => setTimeout(resolve, waitTimeInMs));
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

module.exports.connectorHandler = (event, context, callback) => {
    global.globalInt = globalInt+1;
    console.log("received - event = " + JSON.stringify(event));

    // received
    /**
[
    {
        "payload": {
            "timestamp": 1562844607000,
            "topic": "mytopic",
            "partition": 1,
            "offset": 43822,
            "key": "key1",
            "value": "value1"
        }
    },
    {
        "payload": {
            "timestamp": 1562844608000,
            "topic": "mytopic",
            "partition": 1,
            "offset": 43823,
            "key": "key2",
            "value": "value2"
        }
    }
]
**/
    // response
   /**
    [
        {
            "payload": {
                "timestamp": 1562844607000,
                "topic": "mytopic",
                "partition": 1,
                "offset": 43822,
                "result": .....
            }
        },
        {
            "payload": {
                "timestamp": 1562844608000,
                "topic": "mytopic",
                "partition": 1,
                "offset": 43823,
                "result": .....
            }
        }
        ....
    ]
    **/
    var response = []
    event.forEach(item => {
            console.log("processing:" + JSON.stringify(item));
            var responseItem = {
                payload: {
                    timestamp: item.payload.timestamp,
                    topic: item.payload.topic,
                    partition: item.payload.partition,
                    offset: item.payload.offset,
                    result: JSON.stringify(item.payload.value)
                }
            }
            response.push(responseItem)

        }

    )

//    const response = {
//        statusCode: 200,
//        body: JSON.stringify({
//            message:   `Hello, the neil time is ${new Date().toTimeString()}.  globalCount is ${globalInt} start: ${globalstart}`,
//        }),
//    };


    callback(null, response);
};

module.exports.neilHandler = (event, context, callback) => {
    global.globalInt = globalInt+1;
    console.log("neil - value1 = " + event);

//    const sleep = (waitTimeInMs) => new Promise(resolve => setTimeout(resolve, waitTimeInMs));
// await sleep(5000);

//    await sleep(10000); // sleep for 10 seconds
    console.log("neil - done = " + event);

    const response = {
        statusCode: 200,
        body: JSON.stringify({
            message:   `Hello, the neil time is ${new Date().toTimeString()}.  globalCount is ${globalInt} start: ${globalstart}`,
        }),
    };

    callback(null, response);
};
module.exports.recordsHandler = (event, context, callback) => {
    global.globalInt = globalInt+1;
    console.log("neil - value1 = " + event);
    const response = {
        statusCode: 200,
        body: JSON.stringify({
            message:   `${event}`,
        }),
    };

    callback(null, response);
};



