# xk6-stomp

This is a [Stomp protocol](https://stomp.github.io/) client library for [k6](https://k6.io),
implemented as an extension using the [xk6](https://github.com/grafana/xk6) system.


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
docker run -p 8161:8161 -p 61613:61613 rmohr/activemq
```

2. Write the test code

```javascript
// test.js
import stomp from 'k6/x/stomp';

// connect to broker
const client = stomp.connect({
    addr: 'localhost:61613',
    timeout: '2s',
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
```

3. Result output:

```shell
$ ./k6 run _examples/test.js 

          /\      |‾‾| /‾‾/   /‾‾/   
     /\  /  \     |  |/  /   /  /    
    /  \/    \    |     (   /   ‾‾\  
   /          \   |  |\  \ |  (‾)  | 
  / __________ \  |__| \__\ \_____/ .io

  execution: local
     script: _examples/test.js
     output: -

  scenarios: (100.00%) 1 scenario, 1 max VUs, 10m30s max duration (incl. graceful stop):
           * default: 1 iterations for each of 1 VUs (maxDuration: 10m0s, gracefulStop: 30s)

INFO[0000] msg Hello xk6-stomp!                          source=console

running (00m00.0s), 0/1 VUs, 1 complete and 0 interrupted iterations
default ✓ [======================================] 1 VUs  00m00.0s/10m0s  1/1 iters, 1 per VU

     █ teardown

     data_received........: 322 B 16 kB/s
     data_sent............: 251 B 12 kB/s
     iteration_duration...: avg=5.23ms min=1.12ms med=5.23ms max=9.33ms p(90)=8.51ms p(95)=8.92ms
     iterations...........: 1     48.05613/s
     stomp_ack_count......: 1     48.05613/s
     stomp_read_count.....: 1     48.05613/s
     stomp_read_time......: avg=6.2ms  min=6.2ms  med=6.2ms  max=6.2ms  p(90)=6.2ms  p(95)=6.2ms 
     stomp_send_count.....: 1     48.05613/s
     stomp_send_time......: avg=5.5µs  min=5.5µs  med=5.5µs  max=5.5µs  p(90)=5.5µs  p(95)=5.5µs
```