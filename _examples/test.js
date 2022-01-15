import stomp from 'k6/x/stomp';

export default function () {
    // connect to broker
    const client = new stomp.Client({
        protocol: 'tcp',
        addr: 'localhost:61613',
        timeout: '2s',
        tls: false,
    });

    // send a message to '/my/destination' with application/json as MIME content-type
    client.send('my/destination', 'application/json', '{"test": "123"}');

    // subscribe to receive messages fom 'my/destination' with the client ack mode
    const subscription = client.subscribe('my/destination', 'client'); // client-individual or auto (default)

    // read the message
    const msg = subscription.read();

    // show the message as a string
    console.log('msg', msg.string());
    
    // ack the message
    client.ack(msg);
    
    // unsubscribe from destination
    subscription.unsubscribe();
    
    // disconnect from broker
    client.disconnect();
}