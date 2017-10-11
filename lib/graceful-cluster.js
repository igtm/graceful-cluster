//var stoppable = require('stoppable');
var cluster = require('cluster');
var numCPUs = require('os').cpus().length;

var GracefulCluster = module.exports;

/*

 Starts node.js cluster with graceful restart/shutdown.

 Params:

 - options.serverFunction        - required, function with worker logic.
 - options.log                   - function, custom log function, console.log used by default.
 - options.shutdownTimeout       - ms, force worker shutdown on SIGTERM timeout.
 - options.disableGraceful       - disable graceful shutdown for faster debug.
 - options.restartOnMemory       - bytes, restart worker on memory usage.
 - options.restartOnTimeout      - ms, restart worker by timer.
 - options.workersCount          - workers count, if not specified `os.cpus().length` will be used.
 - options.minimumWorkerProcessHealthPercent  - represents a lower limit on the number of worker servers that must remain in the RUNNING state during restarting, as a percentage of the desired number of servers .
 - options.workerProcessShutdownTimeout  - worker process graceful showdown timeout.

 Graceful restart performed by USR2 signal:

 pkill -USR2 <cluster_process_name>

 or

 kill -s SIGUSR2 <cluster_pid>

 */
GracefulCluster.start = function(options) {

    var serverFunction = options.serverFunction;

    if (!serverFunction) {
        throw new Error('Graceful cluster: `options.serverFunction` required.');
    }

    var exitFunction = options.exitFunction || (function gracefulClusterExit () { process.exit(0); });    
    var log = options.log || console.log;
    var shutdownTimeout = options.shutdownTimeout || 5000;
    var disableGraceful = options.disableGraceful;
    var workersCount = options.workersCount || numCPUs;
    var minimumWorkerProcessHealthPercent = options.minimumWorkerProcessHealthPercent || 50;
    var workerProcessShutdownTimeout = options.workerProcessShutdownTimeout || 30000;

    var closingServerPids = [];

    if (cluster.isMaster) {
        
        var sigkill = false;

        // Create fork with 'on restart' message event listener.
        function fork() {
            const worker = cluster.fork()
            worker.on('message', function(message) {
                if (message.cmd === 'confirm-restart') {
                    const a = (Object.keys(cluster.workers).length - closingServerPids.length) / workersCount;
                    const b = minimumWorkerProcessHealthPercent / 100;
                    console.log(a, b);
                    if (a > b) {
                        worker.send('restart');
                        closingServerPids.push(message.pid);                       
                    }
                }
            });
        }

        // Fork workers.
        for (var i = 0; i < workersCount; i++) {
            fork();
        }

        // Check if has alive workers and exit.
        function checkIfNoWorkersAndExit() {
            if (Object.keys(cluster.workers).length === 0) {
                log('Cluster graceful shutdown: done.');
                if (shutdownTimer) clearTimeout(shutdownTimer);
                exitFunction();
            } else {
                log('Cluster graceful shutdown: wait ' + Object.keys(cluster.workers).length + ' worker' + (Object.keys(cluster.workers).length > 1 ? 's' : '') + '.');
            }
        }

        function startShutdown() {
            
            if (disableGraceful) {
                if (shutdownTimer) clearTimeout(shutdownTimer);
                exitFunction();
                return;
            }

            checkIfNoWorkersAndExit();

            if (sigkill) {
                return;
            }

            // Shutdown timeout.
            shutdownTimer = setTimeout(function() {
                log('Cluster graceful shutdown: timeout, force exit.');
                exitFunction();
            }, shutdownTimeout);

            // Shutdown mode.
            sigkill = true;

            for (var id in cluster.workers) {
                // Send SIGTERM signal to all workers. SIGTERM starts graceful shutdown of worker inside it.
                process.kill(cluster.workers[id].process.pid);
            }
        }
        process.on('SIGTERM',startShutdown);
        process.on('SIGINT',startShutdown);

        cluster.on('fork', function(worker) {
            log('Cluster: worker ' + worker.process.pid + ' started.');
            closingServerPids.pop();
        });

        cluster.on('exit', function(worker, code, signal) {
            if (sigkill) {
                checkIfNoWorkersAndExit();                
                return;
            }
            log('Cluster: worker ' + worker.process.pid + ' died (code: ' + code + '), restarting...');
            fork();
        });

        process.on('uncaughtException', function(err) {
            if (disableGraceful) {
                log('Cluster error:', err.stack);
            } else {
                log('Cluster error:', err.message);
            }
        });

    } else {
        // Start worker.
        //const server = stoppable(serverFunction(), workerProcessShutdownTimeout);
        const server = serverFunction();

        // Self restart logic.
        // Worker process showdown timeout.
        server.on('close', function() { process.exit(); });
        process.on('message', (msg) => {
            if (msg === 'restart') {
                server.close();
                setTimeout(function() {
                    log('Cluster graceful shutdown: timeout, force exit.');
                    process.exit();
                }, workerProcessShutdownTimeout);
            }
        });

        if (options.restartOnMemory) {
            setInterval(function() {
                var mem = process.memoryUsage().rss;
                if (mem > options.restartOnMemory) {
                    console.log(`++++++++++++++++++++++++ ${server._connections}`);
                    log('Cluster: worker ' + process.pid + ' used too much memory (' + Math.round(mem / (1024*1024)) + ' MB), restarting...');
                    process.send({
                        cmd: 'confirm-restart',
                        pid: process.pid
                    });
                }
            }, 1000);
        }

        if (options.restartOnTimeout) {
            setInterval(function() {
                log('Cluster: worker ' + process.pid + ' restarting by timer...');
                process.send({
                    cmd: 'confirm-restart',
                    pid: process.pid
                });
            }, options.restartOnTimeout);
        }
    }
};