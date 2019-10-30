const kafka = require('kafka-node');
const kafka_topic = 'cat';


function randomIntInc(low, high) {
  return Math.floor(Math.random() * (high - low + 1) + low)
}


try {
  const Producer = kafka.Producer;
  const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
  const producer = new Producer(client);

console.log("Producer Initialised..");



  producer.on('ready', function() {
      let num = randomIntInc(10, 1333);
      setInterval(() => {
        let payloads = [
            {
              topic: 'cat',
              messages: num
            }
          ];
          producer.send(payloads, (err, data) => {
            if (err) {
              console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
            } else {
              console.log('[kafka-producer -> '+kafka_topic+']: broker update success >> ' + num);
            }
          });
          num = randomIntInc(10, 1333);
      }, 2000);

  });

  producer.on('error', function(err) {
    console.log(err);
    console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
    throw err;
  });
}
catch(e) {
  console.log(e);
}