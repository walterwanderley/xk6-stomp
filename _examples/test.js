import stomp from 'k6/x/stomp';

// connect to broker
const client = stomp.connect({
    addr: 'localhost:61613',
    timeout: '2s'
});

export default function () {
    // send a message to '/my/destination' with text/plain as MIME content-type
    client.send('my/destination', 'text/plain', 'Hello xk6-stomp!');

    const subscribeOpts = {
        ack: 'client' // client-individual or auto (default)
    }
    // subscribe to receive messages from 'my/destination' with the client ack mode
    const subscription = client.subscribe('my/destination', subscribeOpts); 

    // read the message
    const msg = subscription.read();

    // show the message as a string
    console.log('msg', msg.string());
    
    // ack the message
    client.ack(msg);
    
    // unsubscribe from destination
    subscription.unsubscribe();
}

export function teardown() {
    // disconnect from broker
    client.disconnect();
}