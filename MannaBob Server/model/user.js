var mogoose = require('mongoose');
var Schema = mongoose.Schema;

var userSchema = new Schema({
		id: String,
		Password: String
		//facebook login
		fbToken: String,
		jsonWebToken: String
});

module.exports = mongoose.model('user',userSchema);
