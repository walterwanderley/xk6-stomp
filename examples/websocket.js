import stomp from 'k6/x/stomp';

export default function () {

    // connect to broker
    const client = stomp.connect({
        protocol: 'ws', // stomp over websocket (using this server: https://github.com/spring-guides/gs-messaging-stomp-websocket/)
        addr: 'localhost:8080',
        path: '/gs-guide-websocket/websocket',
        timeout: '2s',
        heartbeat: {
            incoming: '30s',
            outgoing: '30s',
        },
        message_send_timeout: '5s',
        receipt_timeout: '10s'
    });

    // subscribe to receive messages from '/topic/greetings' with auto ack
    let subscription = client.subscribe('/topic/greetings'); 

    // send a message to '/app/hello' with application/json as MIME content-type
    let payload = {name: `WW ${__VU}_${__ITER}`};
    client.send('/app/hello', 'application/json', JSON.stringify(payload));

    // read the message
    const msg = subscription.read();

    // show the message
    console.log('msg', msg.json().content);

    // disconnect from websocket
    client.disconnect();
}
