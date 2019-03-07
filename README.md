# Lab 4 LSDS 

### Oriol Moron - 183953

### Óscar Subirats - 183850

### Sergi Olives - 193196

Fourth assignment of Large Scaled Distributed Systems

bucket link: https://s3.console.aws.amazon.com/s3/buckets/upf.edu.ldsd2018.lab4.grp17/?region=us-east-1&tab=overview

## Command Lines:
* TwitterAccumulator: spark-submit --master local --class upf.edu.lsds2018.lab4.TwitterAccumulator .\lsds2018.lab4-1.0-SNAPSHOT.jar <input>
* TwitterBroadcastWithResource: spark-submit --master local --class upf.edu.lsds2018.lab4.TwitterBroadcastWithResource .\lsds2018.lab4-1.0-SNAPSHOT.jar <input> <output>
* TwitterJoin: spark-submit --master local --class upf.edu.lsds2018.lab4.TwitterJoin .\lsds2018.lab4-1.0-SNAPSHOT.jar <input> <map> <output>

** All executables are inside folder /target

### Accumulator
* The number of tweets that not parsed correctly are 74189

* Content of tweets which didn’t parse correctly (15):
** Total tweets: 1217479
** Parsing attempts: 1291668
** Failed attempts: 74189
** Erroring Tweets content:
** {"limit":{"track":1,"timestamp_ms":"1526148056161"}}
** {"limit":{"track":1,"timestamp_ms":"1526150506242"}}
** {"limit":{"track":2,"timestamp_ms":"1526150538989"}}
** {"limit":{"track":1,"timestamp_ms":"1526150665107"}}
** {"limit":{"track":4,"timestamp_ms":"1526150719217"}}
** {"limit":{"track":5,"timestamp_ms":"1526150881065"}}
** {"limit":{"track":1,"timestamp_ms":"1526151003951"}}
** {"limit":{"track":3,"timestamp_ms":"1526151004881"}}
** {"limit":{"track":6,"timestamp_ms":"1526151007256"}}
** {"limit":{"track":5,"timestamp_ms":"1526151009858"}}
** {"limit":{"track":6,"timestamp_ms":"1526151023106"}}
** {"limit":{"track":3,"timestamp_ms":"1526151023945"}}
** {"limit":{"track":6,"timestamp_ms":"1526151027337"}}
** {"limit":{"track":4,"timestamp_ms":"1526151050991"}}
** {"limit":{"track":8,"timestamp_ms":"1526151078926"}}

### Broadcast
* The 10 most tweeted languages are:
** Spanish; Castilian, 509435
** English, 446603
** French, 54909
** Undetermined, 38932
** Russian, 38290
** Portuguese, 37623
** Italian, 28951
** German, 10733
** Dutch; Flemish, 8901
** Polish, 6885

* The 10 least tweeted languages are:
** Bengali, 1
** Armenian, 7
** Chinese, 14
** Korean, 26
** Persian, 68
** Vietnamese, 76
** Thai, 90
** Hindi, 103
** Serbian, 116
** Arabic, 131

* The number of tweets that have an unidentified/unknown language are 38932

* Ratio of unidentified/unknown language: 0.03197755361694124

### Join
* The 10 most tweeted languages are:
** Spanish; Castilian, 509435
** English, 446603
** French, 54909
** Undetermined, 38932
** Russian, 38290
** Portuguese, 37623
** Italian, 28951
** German, 21466
** Dutch; Flemish, 17802
** Greek, Modern (1453-), 13452

* The 10 least tweeted languages are:
** Bengali, 1
** Armenian, 14
** Korean, 26
** Chinese, 28
** Vietnamese, 76
** Thai, 90
** Hindi, 103
** Serbian, 116
** Arabic, 131
** Persian, 136

* The number of tweets that have an unidentified/unknown language are 38932

* Ratio of unidentified/unknown language: 0.029889270280167763
