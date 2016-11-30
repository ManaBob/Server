var app = require('exxpress')();
var http = require('http');
var server = require('http').createServer();
var io = require('socket.io').listen(server);

var users = {};

 //chatting check connection
io.sockets.on('connection', function (socket) {
    
		//check is login user
    socket.on('loginUser', function (username) {

        console.log('User enter to function loginUser');
        
        if (username in users) {
            socket.emit('ErrorLogin', { msg: 'Usernam is in used' });
            console.log('User Name:' + username + ' is logged');
            users[socket.nickname].emit('LoginState', { state: false });

        }
		 else {
            
            socket.nickname = username;
            users[socket.nickname] = socket
          //  users[socket.nickname].emit('LoginState', { state:true });
            console.log('Loggin successfull');
            users[socket.nickname].emit('LoginState', { state: true });
		 	
        }
		socket.on('connection',function(socket){
			consle.log('a user connected');
			soket.on('disconnect',function(){
					console.log('user disconnected');
		});
		// message communication
		socket.on('message', function(data){
					console.log('received: ' + JSON.stringify(data));
				//[TODO] json file broadcast
					socket.emit('news_response', { hello : 'world'});
				});

					socket.on('news',function(data){
									console.log('received news');
									socket.emit('news_response',{hello:'world'});
									socket.json.send({foo:'json'});
									socket.send('ThisIsAMessage');
								});

					socket.on('disconnect',function(){
									console.log('disconnected');
								});	
    });
    

})

server.listen(3000);





/*
http.createServer(function (req, res) {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Hello World\n');
}).listen(port);

*/
