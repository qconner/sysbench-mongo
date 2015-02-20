'use strict';
 
load('/Users/quentin/src/mongo/jstests/libs/parallelTester.js');
//load('parallelTester.js');

function insert(myCollection, desired) {
    var batchSize = 1000
    print(myCollection)
    var count = 0
    var mydb = db.getSiblingDB('sbtest')
    while (count < desired) {
        var bulk = mydb.getCollection(myCollection).initializeUnorderedBulkOp();
        for (var j=0; j < batchSize; count++, j++) {
            var k = Math.round(Random.rand()*10000000)
            var c = sysbenchString()
            var pad = '' + Math.round(Random.rand()*100000000000)
            for (var y = 0; y < 4; y++)
                pad += '-' + Math.round(Random.rand()*100000000000)
            var d = { _id: count, k: k, c: c, pad: pad }
            //print(tojson(d))
            bulk.insert(d)
        }
        bulk.execute();
    }
    // create indexs
    mydb.getCollection(myCollection).ensureIndex({k: 1})

    function sysbenchString() {
        var s = '' + Math.round(Random.rand()*100000000000)
        for (var x = 0; x < 8; x++)
            s += '-' + Math.round(Random.rand()*100000000000)
        return s
    }

}


function simulate_sysbench_load(num_collections, num_docs_per_collection) {
    // data load phase
    print("creating data for", num_collections, " collections")

    // drop old collections
    print('dropping old collections:')
    var mydb = db.getSiblingDB('sbtest')
    for (var offset=1; offset <= num_collections; offset++) {
        var s = 'sbtest' + offset
        print(s)
        mydb.getCollection(s).drop()
    }

    var threads = []

    print('\nloading new collections:')
    for (var offset=0; offset < num_collections; offset++) {
        var n = offset + 1
        var s = 'sbtest' + n
        threads[offset] = new ScopedThread(insert, s, num_docs_per_collection)
        threads[offset].start()
    }

    print("all threads started")
    threads.forEach(function(t) {
        t.join();
    });
    threads = []
}



// load data
var collection_count = 16
var docs_per_collection = 20000000
//var docs_per_collection = 8000
simulate_sysbench_load(collection_count, docs_per_collection)

 



