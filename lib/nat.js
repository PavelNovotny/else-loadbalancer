/**
 *
 * Created by pavelnovotny on 12.10.16.
 */
const net = require('net');

const servers1 = [
    {host:"10.199.160.19", port: 9200, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 10200, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 10201, status: {ok: true, timestamp:0}}
];
const servers = [
    {host:"10.199.160.19", port: 9200, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 4444, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
    ,{host:"10.199.160.19", port: 5555, status: {ok: true, timestamp:0}}
];
const failServer = {host:"localhost", port: 5555};

const timestamp = new Date();
const clients = {};
var clientNum = 0;
checkClients();
checkNodeHealth();

const server = net.createServer((connection) => {
    handleIncomingConnection(connection);
});

server.on('error', (err) => {
    throw err;
});

server.listen(8124, () => {
    console.log('---------------  server bound --------------------');
});

function handleIncomingConnection(connection) {
    const clNum = registerClient(clientNum++, connection);
    connection.on('data', (data) => {
        checkClientReady(clNum, data);
    });
    connection.on('end', () => {
        console.log('(' + clNum +')' + '----------------  server: client disconnected -----------------------------');
        if (client.remoteAddress != "undefined") {
            client.end();
        }
    });
}

function checkClientReady(clNum, data) {
    if (clients[clNum] === undefined) {
        console.log('(' + clNum +')' + '----------------  client not ready yet -----------------------------');
        setTimeout(function (){
            checkClientReady(clNum, data);
        }, 200);
    } else {
        const client = clients[clNum].client;
        client.write(data);
        clients[clNum].epoch = timestamp.getTime();
        console.log('(' + clNum +')' + '----------------- received data from client, resendig them to elasticsearch --------------------');
        console.log(data.toString());
    }
}

function registerClient(clNum, connection) {
    const server = loadBalance();
    const client = net.createConnection(server, () => {
        console.log('(' + clNum +')' + '----------------- connected to elasticsearch --------------------');
    });
    client.on('connect', () => {
        console.log('(' + clNum +')' + '------------- client connected --------------');
        clients[clNum] = {"client": client, epoch: timestamp.getTime()};
    });
    client.on('data', (data) => {
        connection.write(data);
        clients[clNum].epoch = timestamp.getTime();
        console.log('(' + clNum +')' + '----------------- received data from elasticsearch, resending it to client --------------------');
        console.log(data.toString());
    });
    client.on('error', () => {
       console.log('(' + clNum +')' + '----------------- ERROR connecting to elasticsearch --------------------');
        server.status.ok = false;
        server.status.timestamp = timestamp.getTime();
        registerClient(clNum, connection);
    });
    client.on('end', () => {
        console.log('(' + clNum +')' + '--------------- disconnected from elasticsearch ------------------------');
        if (connection.remoteAddress != "undefined") {
            connection.destroy();
        }
    });
    return clNum;
}

function loadBalance() {
    var index = randomIndex(servers.length);
    const server = servers[index];
    if (!server.status.ok) {
        console.log("Loadbalance to NOK server prohibited:" + index + "," + JSON.stringify(server));
        var atLeastOneOK = false;
        for (var i=0;i<servers.length;i++) {
            atLeastOneOK = atLeastOneOK || servers[i].status.ok;
        }
        if (atLeastOneOK) {
            return loadBalance();
        } else {
            console.log("ALL ELASTICSEARCH SERVERS DOWN, loadbalancing to failServer.");
            return failServer;
        }
    }
    console.log("Loadbalancing to a server:" + index + "," + JSON.stringify(server));
    return server;
    //je server naživu? Z historie dotazů na zdraví.
    //co dělat, když dotaz nedopadne? - connection refused je jasné. Ale co když vrátí data, že je v prdeli?
}

function checkClients() {
    //periodicky kontrolovat klienty, a pokud tam dlouho visí neaktivní connection, tak ji zavřít.
    setInterval(function() {
        var currentTime = timestamp.getTime();
        for (var key in clients) {
            if (currentTime - clients[key].epoch > 30000) { //30-40 sec
                clients[key].client.destroy();
                delete clients[key];
            }
        }
    }, 10000);
}

function checkNodeHealth() {
    //periodicky kontrolovat zdraví nodů a někam zapisovat a logovat.
}


function randomIndex(indexSize) {
    return Math.floor((Math.random() * (indexSize)));
}
