var mysql = require('mysql'),
    util = require('util'),
    async = require('async'),
    sleep = require('sleep'),
    twitter = require('twitter');
require('date-utils');

var MeCab = new require('mecab-async')
      , mecab = new MeCab();

var client_m = mysql.createConnection({
        user: "twitte",
        password: "",
        host: "localhost",
        port: 3306,
        database: "twitter",
});

var tw = new twitter({
    consumer_key: '',
    consumer_secret: '',
    access_token_key: '',
    access_token_secret: ''
});

var sqltw = "select count(id) cnt from user_timeline where tweet_id = ?";
var sqlins = "insert into user_timeline (tweet_id, username, body, wakachi, create_at) values (?, ?, ?, ?, ?)";
var n = 0;
var max_id = 0;

async.timesSeries(10000, function(n, next){

  console.log(n);

  async.series([
    function (callbackoya) {
        if(n!=0 && (n % 10) == 0){
          console.log("sleep");
          sleep.sleep(70);
          callbackoya();
        }else{
          callbackoya();
        }
     },
		function (callbackoya) {

        console.log("get tweet");
        tw.get('/search/tweets.json', {"q":"-bot", "lang":"ja", "locale":"ja", "count":200 , "max_id":max_id}, function(data) {
            async.eachSeries(data.statuses , function(item, callback){


              var d = new Date(item.created_at);
              var datetime = d.addHours(9).toFormat('YYYY-MM-DD HH24:MI:SS');
              var name = item.user.screen_name;
              var desc = item.user.description;
              var text = item.text;
              var id = item.id;
              max_id = item.id;
              console.log(max_id);

              //botは弾く
              if(!desc.match(/bot/i) ){

                //RT
                if(!text.match(/@/) ){

                  client_m.query(sqltw, [id], function(err, result2, fields) {

                    if(result2[0].cnt == 0){

                      text = text.replace(/\)/g,' ').replace(/\(/g,' ').replace(/\n/g,' ').replace(/\r/g,' ');
                      var wakachi = mecab.wakachiSync(text);

                      console.log(id, name, text, wakachi.join(' '),   datetime);

                      client_m.query(sqlins ,[id, name, text, wakachi.join(' '), datetime],function(err) {
                          callback();
                      });

                    }else{
                          callback();
                    }
                  });

                }else{
                  console.log("RT");
		              callback();
                }

              }else{
                 console.log("bot");
                 callback();
              }

            }, function(err){
              if(err) throw err;
              console.log("next",n);
              callbackoya();

            });
        });
    },
    function (callbackoya) {
         console.log("next");
         next();
     }
    ], function (err, results) {
        if(err) throw err;
    });
}, function(err) {
  console.log("end");
});

