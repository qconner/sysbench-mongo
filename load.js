
function insert(myCollection, desired, randSeed) {
    var batchSize = 1000

    print('bulk loading ' + desired + ' documents to ' + myCollection + ' with batch size of ' + batchSize)
    var count = 0

    var mydb = db.getSiblingDB('sbtest')

    var seed = randSeed

    while (count < desired) {
        var bulk = mydb.getCollection(myCollection).initializeUnorderedBulkOp();
        for (var j=0; j < batchSize; count++, j++) {
            var k = Math.round(random()*10000000)
            var c = sysbenchString()
            var pad = '' + Math.round(random()*100000000000)
            for (var y = 0; y < 4; y++)
                pad += '-' + Math.round(random()*100000000000)
            var d = { _id: count, k: k, c: c, pad: pad }
            //print(tojson(d))
            bulk.insert(d)
        }
        bulk.execute();
    }

    // create indexes
    print('creating index on', myCollection)
    mydb.getCollection(myCollection).ensureIndex({k: 1})

    function sysbenchString() {
        var s = '' + Math.round(random()*100000000000)
        for (var x = 0; x < 8; x++)
            s += '-' + Math.round(random()*100000000000)
        return s
    }

    // PRNG borrowed from http://stackoverflow.com/questions/521295/javascript-random-seeds
    function random() {
        // avoid zero and PI
        if (seed >= Number.MAX_SAFE_INTEGER || seed < 0.000000001 || Math.abs(seed - Math.PI) < 0.000000001)
            seed = 1
        var x = Math.sin(seed++) * 10000
        return x - Math.floor(x)
    }
}


function simulate_sysbench_load(num_collections, num_docs_per_collection) {
    // data load phase
    print("creating data for", num_collections, "collections")

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
    var t = Math.round(Date.now() / num_collections)
    for (var offset=0; offset < num_collections; offset++) {
        var n = offset + 1
        var s = 'sbtest' + n
        threads[offset] = new ScopedThread(insert, s, num_docs_per_collection, (offset + 1) * t)
        threads[offset].start()
    }

    print("all threads started")
    threads.forEach(function(t) {
        t.join();
    });
    threads = []
}



