# xk6-stomp

This is a [Stomp protocol](https://stomp.github.io/) client library for [k6](https://github.com/k6io/k6),
implemented as an extension using the [xk6](https://github.com/k6io/xk6) system.


## Build

To build a `k6` binary with this extension, first ensure you have the prerequisites:

- [Go toolchain](https://go101.org/article/go-toolchain.html)
- Git

Then:

1. Install `xk6`:
  ```shell
  go install go.k6.io/xk6/cmd/xk6@latest
  ```

2. Build the binary:
  ```shell
  xk6 build --with github.com/walterwanderley/xk6-stomp
  ```

## Example test script

1. Start a [Stomp server](https://stomp.github.io/implementations.html#STOMP_Servers) (ActiveMQ, RabbitMQ, etc)

```shell
docker run -p 61616:61616 -p 8161:8161 -p 61613:61613 rmohr/activemq
```

2. Write the test code

```javascript
// test.js
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

    // show the message
    console.log('msg', msg.string());
    
    // ack the message
    client.ack(msg);
    
    // unsubscribe from destination
    subscription.unsubscribe();
    
    // disconnect from broker
    client.disconnect();
}
```

3. Result output:

```shell
$ ./k6 run test.js

          /\      |‾‾| /‾‾/   /‾‾/
     /\  /  \     |  |/  /   /  /
    /  \/    \    |     (   /   ‾‾\
   /          \   |  |\  \ |  (‾)  |
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: test.js
     output: -

  scenarios: (100.00%) 1 scenario, 1 max VUs, 10m30s max duration (incl. graceful stop):
           * default: 1 iterations for each of 1 VUs (maxDuration: 10m0s, gracefulStop: 30s)

INFO[0000] msg {"test": "123"}                           source=console

running (00m00.4s), 0/1 VUs, 1 complete and 0 interrupted iterations
default ✓ [======================================] 1 VUs  00m00.4s/10m0s  1/1 iters, 1 per VU
```