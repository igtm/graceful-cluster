# Awesome Graceful Cluster
- This repo is folked from [graceful-cluster](https://github.com/itteco/graceful-cluster) to add support of graceful server shotdown (if remaing connection, not shutting down soon).
- This repo is very EXPERIMENTAL and not well documented yet. if you consider to use in production, please be careful.
- I only change 'graceful-cluster.js' file from original source code;

Install:

    npm install awesome-graceful-cluster
    
## How to use

### 1. Enable graceful server shutdown

This patch will prevent active connections reset when server receives `SIGKILL` or `SIGTERM`. Idle (keep-alive) inbound connections without active requests will be destroyed.
 
Example 'server.js':

```js
// Example server with 'express'.
var express = require('express');
var app = express();
var listener = app.listen(8000);

var GracefulServer = require('awesome-graceful-cluster').GracefulServer;
var gracefulServer = new GracefulServer({
    server: listener,
    shutdownTimeout: 10 * 1000,             // 10 sec.
});
```
    
GracefulServer options description:

| option                   | info
| ------------------------ | ---
|`log`                     | function, custom log function, `console.log` used by default.
|`server`                  | required, http server instance.
|`shutdownTimeout`         | ms, force worker shutdown on `SIGTERM` timeout. Defaults to 5000ms.
 
Also you can initiate graceful shutdown when needed:

```js
gracefulServer.shutdown();
```

### 2. Use simplified cluster initialization

This cluster wrapper will send `SIGTERM` signal to workers and wait till they finished all requests.

Also it can gracefully restart all workers one by one with zero cluster downtime on some conditions:

  1. Worker memory used.
  2. Worker time online.
  3. Your custom condition: just call `GracefulCluster.gracefullyRestartCurrentWorker()` to restart current worker in `serverFunction`.
  4. On `SIGUSR2` signal to cluster process.

Example 'cluster.js':

```js
var GracefulCluster = require('awesome-graceful-cluster').GracefulCluster;

process.title = '<your-cluster-title>';     // Note, process title must be near filename (cluster.js) length, longer title truncated.

GracefulCluster.start({
    shutdownTimeout: 10 * 1000,             // 10 sec.
    restartOnTimeout: 5 * 3600 * 1000,      // 5 hours.
    restartOnMemory: 150 * 1024 * 1024,     // 150 MB.
    serverFunction: function() {  // Your 'server.js' code module with server logic. you must return [server, app]
        var express = require('express');
        var app = express();
        var server = app.listen(8000);
        return [server, app];
    }         
});
```

Example 'app.js': (insert kind of below code so that you can close existing connection)

```js
...
    app.use(function(req, res, next){
        if (app.get('graceful_shutdown') === true) {
            console.log('Connection:close header was sent.')
            res.set('Connection', 'close');
        }
        next();
    });
...
```

GracefulCluster options description:

| option                   | info
| ------------------------ | ---
| `disableGraceful`        | disable graceful shutdown for faster debug.
| `exitFunction`           | optional, function that is called when the master needs to exit. The default function exits with exit code 0.
| `log`                    | function, custom log function, `console.log` used by default.
| `restartOnMemory`        | bytes, optional. restart worker on memory usage.
| `restartOnTimeout`       | ms, optional. restart worker by timer.
| `serverFunction`         | **required**, function with worker logic.
| `shutdownTimeout`        | ms, optional. force worker shutdown on `SIGTERM` timeout. Defaults to 5000ms.
| `workersCount`           | workers count, if not specified `os.cpus().length` will be used.
| `minimumWorkerProcessHealthPercent`  | represents a lower limit on the number of worker servers that must remain in the RUNNING state during restarting, as a percentage of the desired number of servers .
| `workerProcessDisconnectDelay`       | worker process graceful showdown delay time.
| `workerProcessForceShutdownTimeout`  | worker process force showdown timeout (after workerProcessDisconnectDelay time).

### Gracefully restart cluster

Graceful restart performed by `USR2` signal:

```sh
pkill -USR2 <your-cluster-title>
```

or

```sh
kill -s SIGUSR2 <cluster-pid>
```
    
This method is also good if your app is launched with [forever](https://github.com/foreverjs/forever):

```sh
forever start cluster.js
```

### Using with PM2

If you prefer [PM2](https://github.com/Unitech/pm2) you should use 'server.js' patch only. This will force PM2 to wait until active connections are closed when using:

```sh
pm2 reload <id>
```

With PM2 graceful reload don`t forget to set important process parameters:

 - `"instances": 0`         - use cluster with multiple instances, so one instance will still work when another is reloaded.
 - `"kill_timeout": 5000`   - wait more time to allow active connections finish their responses.