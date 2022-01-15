import stomp from 'k6/x/stomp';

// connect to broker
const client = new stomp.Client({
    addr: 'localhost:61613',
    timeout: '2s'
});

export default function () {

    let payload = {
        test: "123"
    } 

    // send a message to '/my/destination' with application/json as MIME content-type
    client.send('my/destination', 'text/plain', JSON.stringify(payload));

    // subscribe to receive messages fom 'my/destination' with the client ack mode
    const subscription = client.subscribe('my/destination', 'client'); // client-individual or auto (default)

    // read the message
    const msg = subscription.read();

    // show the message
    console.log('msg', msg.json().test);
    
    // ack the message
    client.ack(msg);
    
    // unsubscribe from destination
    subscription.unsubscribe();
}

export function teardown() {
    // disconnect from broker
    client.disconnect();
}