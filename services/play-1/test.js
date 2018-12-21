

// {"keySchema":{"optional":false},"valueSchema":{"optional":false},"topic":"aws-lambda-topic","kafkaPartition":1,"timeStampType":"CreateTime","records":[{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0}]}


function testJsonParsing() {

    var testPayload = {"keySchema":{"optional":false},"valueSchema":{"optional":false},"topic":"aws-lambda-topic","kafkaPartition":1,"timeStampType":"CreateTime","records":[{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0}]};

    module.exports.neilHandler(testPayload, null, null);
    // var x = 5;
    // var y = 1;
    // var sum1 = x + y;
    // var sum2 = addTwoNumbers(x, y);
    //
    // console.log('addTwoNumbers() should return the sum of its two parameters.');
    // console.log('Expect ' + sum1 + ' to equal ' + sum2 + '.');
    //
    // try {
    //
    //     assert.equal(sum1, sum2);
    //
    //     console.log('Passed.');
    // } catch (error) {
    //     console.error('Failed.');
    // }
}

testJsonParsing();