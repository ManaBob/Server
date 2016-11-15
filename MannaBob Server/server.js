svar http = require('http');
var port = process.env.port || 3000;
var server = require('http').createServer();
var io = require('socket.io').listen(server);

var users = {};

io.sockets.on('connection', function (socket) {
    
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

    });
    

})


server.listen(port);





/*
http.createServer(function (req, res) {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Hello World\n');
}).listen(port);

*/
