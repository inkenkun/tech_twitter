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

var sql = "select id,username from girls order by id desc";
var sqltw = "select count(id) cnt from girls_timeline where tweet_id = ?";
var sqlwc = "select id from girls_word where word = ?";
var sqlins = "insert into girls_timeline ( tweet_id, username, body, wakachi, create_at) values ( ?, ?, ?, ?, ?)";
var sqlinstw = "insert into girls_timeline_word (t_id, w_id) values (?, ?)";
var sqlinsw = "insert into girls_word (word,hindo) values (?,1)";
var sqlupw = "update girls_word set hindo = hindo + 1 where word = ?";
var n = 0;

client_m.query(sql, function(err, result, fields) {

    async.eachSeries(result , function(item, callbackoya){

	    var name = item.username;
      console.log("id:",item.id);

      async.series([
          function (callback2) {

            console.log(n);
            if(n!=0 && (n % 10) == 0){
              console.log("sleep");
              sleep.sleep(70);
              callback2();
            }else{
              callback2();
            }
          },
          function (callback2) {

            tw.get('/statuses/user_timeline.json', {"screen_name":name, "count":200}, function(data) {

              n++;
              async.eachSeries(data , function(item2, callback){

                var d = new Date(item2.created_at);
                var datetime = d.addHours(9).toFormat('YYYY-MM-DD HH24:MI:SS');
                var text = item2.text;
                var id = item2.id;

                console.log(id);

                //RT
                if(!text.match(/@/) ){

                  client_m.query(sqltw, [id], function(err, result2, fields) {

                    if(result2[0].cnt == 0){

                        text = text.replace(/\)/g,' ').replace(/\(/g,' ').replace(/\n/g,' ').replace(/\r/g,' ');
                        var wakachi = mecab.parseSync(text);
                        var wakachi_text = mecab.wakachiSync(text);
                        if(!wakachi_text.join(' ')){
                          console.log("no",wakachi_text,wakachi,'"'+text+'"');
                        }
			                  client_m.query(sqlins ,[ id , name ,text, wakachi_text.join(' '), datetime] ,function(err, result_id) {
                          console.log('insert');

                          client_m.query("SELECT LAST_INSERT_ID()",function(err,result_id,fields){
                            console.log("last",result_id[0]["LAST_INSERT_ID()"]);

                            var timeline_id = result_id[0]["LAST_INSERT_ID()"];

                            async.eachSeries(wakachi , function(tango_set, callbackwaka){

                                var tango = tango_set[0];
                                var type = tango_set[1];

                                if(tango.length == 1  || tango == "co" || tango == "www" || tango.match(/\//) || tango.match(/http/)){
                                    console.log("noize",tango);
                                    callbackwaka();
                                }else{

                                  if(type == '名詞' || type == '動詞' || type == '副詞') {
                                    client_m.query(sqlwc, [tango], function(err, result3, fields) {
                                      if(!result3[0]){
                                          client_m.query(sqlinsw ,[tango],function(err,result_id) {
                                              client_m.query("SELECT LAST_INSERT_ID()",function(err,result_id,fields){

                                                var word_id = result_id[0]["LAST_INSERT_ID()"];
                                                console.log(err,'insert word');

                                                client_m.query(sqlinstw ,[timeline_id,word_id],function(err) {
                                                    callbackwaka();
                                                });
                                              });
                                          });
    					                        }else{
                                          client_m.query(sqlupw ,[tango],function(err) {
                                                console.log(err,'update word');

                                                var word_id = result3[0].id;
                                                client_m.query(sqlinstw ,[timeline_id,word_id],function(err) {
                                                    callbackwaka();
                                                });
                                          });
                                      }
                                    });
                                  }else{
                                          callbackwaka();
                                  }

                                }

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
