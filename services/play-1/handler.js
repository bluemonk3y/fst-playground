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

module.exports.neilHandler = (event, context, callback) => {
    global.globalInt = globalInt+1;
    console.log("neil - value1 = " + event);
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



