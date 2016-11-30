var express = require('express');
var router = express.Router();

var users = require('./../model/user');
var jwt = require('jsonwebtoken');
var jwtSecret = 'secret';

router.post('/login/local', function (req, res, next) {
		var localID = req.body.id;
		var localPassword = req.body.password;

		var findConditionLocalUser = {
				id: localEmail,
				localPassword: localPassword
		}
		users.findOne(findConditionLocalUser)
				.exec(function (err, user) {
						if (err){
								res.json({
										type: false,
										data: "Error occured " + err
								});
						} else if (!user) {
								res.json({
										type: false,
										data: "Incorrect id/password"
								});
						} else if(user) {
								res.json({
										type: true,
										data: user,
										token: user.jsonWebToken
								});
						}
				});
});

router.post('/login/local/signup', function (req, res, next) {
		var localID = req.body.email;
		var localPassword = req.body.password;
		console.log(localID);
		console.log(localPassword);

		var findConditionLocalUser = {
				id: localID
		}

		users.findOne(findConditionLocalUser)
				.exec(function (err, user) {
						if (err){
								res.json({
										type: false,
										data: "Error occured " + err
								});
						} else if (user) {
								res.json({
										type: false,
										data: "id already exists"
								});
						} else if(!user) {
								localSignup(localID, localPassword, function (err, savedUser) {
										if (err){
												res.json({
														type: false,
														data: "Error occured " + err
												});
										} else {
												res.json({
														type: true,
														data: savedUser,
														token: savedUser.jsonWebToken
												});
										} 
								});
						}
				})
});

function localSignup(userId, userPassword, next) {
		var userModel = new users();
		userModel.id = userId;
		userModel.localPassword = userPassword;
		console.log(userModel);
		userModel.save(function (err, newUser) {
				newUser.jsonWebToken = jwt.sign(newUser, jwtSecret);
				newUser.save(function (err, savedUser) {
						next(err, savedUser);
				});
		});
}

router.get('/me', ensureAuthorized, function (req, res, next) {
		var findConditionToken = {
				jsonWebToken: req.token
		};
		console.log(req.token);
		users.findOne(findConditionToken, function (err, user) {
				if (err){
						res.json({
								type: false,
								data: "Error occured: " + err
						});
				} else {
						console.log("me : " + user);
						res.json({
								type: true,
								data: user
						});
				}
		})
});

function ensureAuthorized(req, res, next) {
		var bearerToken;
		var bearerHeader = req.headers["authorization"];
		console.log(bearerHeader);
		if (typeof bearerHeader !== "undefined") {
				var bearer = bearerHeader.split(" ");
				bearerToken = bearer[1];
				req.token = bearerToken;
				next();
		} else {
				res.send(403);
		}
}
