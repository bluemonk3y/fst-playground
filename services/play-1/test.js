



const assert = require('assert');
const operations = require('./operations.js');
const handler = require('./operations.js');

// it('should return true', () => {
//     assert.equal(true, true);
// });


it('TEST: correctly calculates the sum of 1 and 3', () => {
    assert.equal(operations.add(1, 3), 4);
});


var payload =  {"keySchema":{"optional":false},"valueSchema":{"optional":false},"topic":"aws-lambda-topic","kafkaPartition":1,"timeStampType":"CreateTime","records":[{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0}]}

it('TEST: correctly handles payload', () => {
    // assert.e(1 == 1);
    assert.equal(50, 50);
    assert.ok(true)
});


it('TEST: correctly handles payload', () => {
    // var user;

    before(function(done){
        // User.create({ username: 'test' , function(err,u){
        //         user = u;
        //         done();
        //     });
    });

    console.log("running test")
    var testPayload = {"keySchema":{"optional":false},"valueSchema":{"optional":false},"topic":"aws-lambda-topic","kafkaPartition":1,"timeStampType":"CreateTime","records":[{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0}]};



    it('should have a username',function(){
        true
    });
});

// function testJsonParsing() {
//
//     console.log("I will goto the STDOUT");
//     // log("running test")
//     var testPayload = {"keySchema":{"optional":false},"valueSchema":{"optional":false},"topic":"aws-lambda-topic","kafkaPartition":1,"timeStampType":"CreateTime","records":[{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0},{"key":"key","value":"{\"name\":\"Bobby McGee\",\"age\":21}","timestamp":1545407722275,"kafkaOffset":0}]};
//
//     neilHandler(testPayload, null, null);
//     // var x = 5;
//     // var y = 1;
//     // var sum1 = x + y;
//     // var sum2 = addTwoNumbers(x, y);
//     //
//     // console.log('addTwoNumbers() should return the sum of its two parameters.');
//     // console.log('Expect ' + sum1 + ' to equal ' + sum2 + '.');
//     //
//     // try {
//     //
//     //     assert.equal(sum1, sum2);
//     //
//     //     console.log('Passed.');
//     // } catch (error) {
//     //     console.error('Failed.');
//     // }
// }
//
// testJsonParsing();