var mysql = require('mysql'),
    util = require('util'),
    async = require('async'),
    sleep = require('sleep'),
    twitter = require('twitter');
require('date-utils');

var MeCab = new require('mecab-async')
  , mecab = new MeCab();

var Zunda = new require('zunda')
	, zunda = new Zunda();

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

var sql = "select username from lady order by username";
var sqllt = "select count(id) cnt from lady_timeline where tweet_id = ?";
var sqltz = "select count(id) cnt from lady_timeline_zunda where tweet_id = ? and body=?";
var sqlins = "insert into lady_timeline ( tweet_id, username, body, wakachi, create_at) values ( ?, ?, ?, ?, ?)";
var sqlinstz = "insert into lady_timeline_zunda (tweet_id, jise, kaso, taido, shingi, kachi, body, wakachi) values (?, ?, ?, ?, ?, ?, ?, ?)";
var n = 0;

client_m.query(sql, function(err, result, fields) {

    async.eachSeries(result , function(item, callbackoya){

	var name = item.username;
      console.log("username:",name);

      async.series([
          function (callback2) {

            console.log(n);
            if(n!=0 && (n % 10) == 0){
              console.log("sleep");
              sleep.sleep(60);
              callback2();
            }else{
              callback2();
            }
          },
          function (callback2) {

            tw.get('/statuses/user_timeline.json', {"screen_name":name, "count":100}, function(data) {

              n++;
              async.eachSeries(data , function(item2, callback){

                var d = new Date(item2.created_at);
                var datetime = d.addHours(9).toFormat('YYYY-MM-DD HH24:MI:SS');
                var text = item2.text;
                var id = item2.id;

                console.log(id);

                //RT
                if(!text.match(/@/) ){

                  client_m.query(sqllt, [id], function(err, result2, fields) {

                    if(result2[0].cnt == 0){

                        text = text.replace(/\)/g,' ').replace(/\(/g,' ').replace(/\n/g,' ').replace(/\r/g,' ');
                        var wakachi = mecab.parseSync(text);
                        var wakachi_text = mecab.wakachiSync(text);
			                  var zunda_text = zunda.parseSync(text);

			                  client_m.query(sqlins ,[ id , name ,text, wakachi_text.join(' '), datetime] ,function(err, result_id) {
                            console.log('insert');

                            client_m.query("SELECT LAST_INSERT_ID()",function(err,result_id,fields){
                                
                                console.log("last",result_id[0]["LAST_INSERT_ID()"]);
                                var timeline_id = result_id[0]["LAST_INSERT_ID()"];

                                async.eachSeries(zunda_text , function(zunda_p, callbackwaka){

                                    var jise = zunda_p.event[3];
                                    var kaso = zunda_p.event[4];
                                    var taido = zunda_p.event[5];
                                    var shingi = zunda_p.event[6];
                                    var kachi = zunda_p.event[7];
    				                        var body = zunda_p.words;
    				                        var wakachi = zunda_p.wakachi;
                                    console.log("zunda:",taido,shingi,kachi,body,wakachi);

        				                    client_m.query(sqltz, [id,body], function(err, result3, fields) {

                                  		if(result3[0].cnt == 0){

                              					client_m.query(sqlinstz ,[id,jise,kaso,taido,shingi,kachi,body,wakachi],function(err,result_id) {				
                              						callbackwaka();				
                              					});
                              				}else{
                              					callbackwaka();
                              				}
                        				    });

                                }, function(err){
                                    if(err) throw err;
                                    console.log("end word");
                                    callback();
                                });
                            });

                        });

                    }else{
                      callback();
                    }
                  });


                }else{
                  console.log("RT");
                  callback();
                }
	            }, function(err){
                if(err) throw err;
                callback2();
              });
            });

        },function (callback2) {
             console.log("next");
             callbackoya();
        }], function (err, results) {
            if(err) throw err;
        });

    }, function(err){
      if(err) throw err;
    });


});
