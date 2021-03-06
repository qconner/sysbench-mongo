

function workload(num_collections, num_docs_per_collection, randSeed) {
    var iterationsDesired = 2000

    var mydb = db.getSiblingDB('sbtest')

    var seed = randSeed

    var count = 0
    while (count < iterationsDesired) {
        pointSelect(mydb, num_collections, num_docs_per_collection)
        simpleRange(mydb, num_collections, num_docs_per_collection)
        sumRange(mydb, num_collections, num_docs_per_collection)
        distinctRange(mydb, num_collections, num_docs_per_collection)
        indexUpdate(mydb, num_collections, num_docs_per_collection)
        nonIndexUpdate(mydb, num_collections, num_docs_per_collection)
        oltpInsert(mydb, num_collections, num_docs_per_collection)

        count++
    }

    function randomCollection(mydb, num_collections) {
        // randomly choose a collection
        var n = Math.round(random() * (num_collections - 1)) + 1
        var myCollection = 'sbtest' + n
        //print('operating on collection: ', myCollection)
        return mydb.getCollection(myCollection)
    }

    function pointSelect(mydb, num_collections, num_docs_per_collection) {
        var point_selects = 10
        var coll = randomCollection(mydb, num_collections)
        // point select (findOne by _id with projection)
        for (var j=0; j < point_selects; j++) {
            var myID = Math.round(random() * (num_docs_per_collection - 1))
            var doc = coll.findOne({_id: myID}, {c: 1, _id: 0})
            if (doc == null)
                print('ps: could not find document with _id', myID, 'in collection', coll)
        }
    }


    function simpleRange(mydb, num_collections, num_docs_per_collection) {
        
        var simple_ranges = 1
        var range_size = 100

        var coll = randomCollection(mydb, num_collections)
        // simple range (find using $gte, $lte and projection)
        for (var j=0; j < simple_ranges; j++) {
            var startID = Math.round(random() * ((num_docs_per_collection - 1) - range_size))
            var endID = startID + range_size - 1

            var curs = coll.find({_id: {"$gte": startID, "$lte": endID}}, {c: 1, _id: 0})
            if (curs == null) {
                print('sr: could not find documents with _id between', startID, 'and', endID, 'in collection', coll)
            }
            else
                while (curs.hasNext())
                    var doc = curs.next()
        }
    }


    function sumRange(mydb, num_collections, num_docs_per_collection) {
        
        var sum_ranges = 1
        var range_size = 100

        var coll = randomCollection(mydb, num_collections)

        // sum range (aggregation using $match, $group, $sum and projection)
        for (var j=0; j < sum_ranges; j++) {
            var startID = Math.round(random() * ((num_docs_per_collection - 1) - range_size))
            var endID = startID + range_size - 1

            var curs = coll.aggregate( [ {$match: {_id: {$gte: startID, $lte: endID}}}, {$project: {k: 1, _id: 0}}, {$group: {_id: null, average: {$sum: "$k"}}} ] )
            if (curs == null) {
                print('mr: could not find documents with _id between', startID, 'and', endID, 'in collection', coll)
            }
            else {
                while (curs.hasNext()) {
                    var doc = curs.next()
                    //print(tojson(doc))
                }
            }
        }
    }


    function distinctRange(mydb, num_collections, num_docs_per_collection) {
        
        var distinct_ranges = 1
        var range_size = 100

        var coll = randomCollection(mydb, num_collections)

        for (var j=0; j < distinct_ranges; j++) {
            var startID = Math.round(random() * ((num_docs_per_collection - 1) - range_size))
            var endID = startID + range_size - 1

            var results = coll.distinct("c", {_id: {"$gte": startID, "$lte": endID}}).sort()
            if (results == null) {
                print('dr: could not find documents with _id between', startID, 'and', endID, 'in collection', coll)
            }
        }
    }

    function indexUpdate(mydb, num_collections, num_docs_per_collection) {
        
        var index_updates = 3

        var coll = randomCollection(mydb, num_collections)

        for (var j=0; j < index_updates; j++) {
            var myID = Math.round(random() * (num_docs_per_collection - 1))
            var writeResult = coll.update({_id: myID}, {$inc: {k: 1}})
            if (writeResult == null) {
                print('iu: update operation failed')
            }
            else if (writeResult.nMatched != 1) {
                print('iu: could not find document with _id', myID, 'in collection', coll)
                print(tojson(writeResult))
            }
            else if (writeResult.nModified != 1) {
                print('iu: could not update document with _id', myID, 'in collection', coll)
                print(tojson(writeResult))
            }
        }
    }

    function nonIndexUpdate(mydb, num_collections, num_docs_per_collection) {
        
        var non_index_updates = 3

        var coll = randomCollection(mydb, num_collections)

        for (var j=0; j < non_index_updates; j++) {
            var myID = Math.round(random() * (num_docs_per_collection - 1))
            var newcval = sysbenchString()
            var writeResult = coll.update({_id: myID}, {$set: {c: newcval}})
            if (writeResult == null) {
                print('niu: update operation failed')
            }
            else if (writeResult.nMatched != 1) {
                print('niu: could not find document with _id', myID, 'in collection', coll)
                print(tojson(writeResult))
            }
            else if (writeResult.nModified != 1) {
                print('niu: could not update document with _id', myID, 'in collection', coll)
                print(tojson(writeResult))
                print("new c value:", newcval)
                print(tojson(coll.findOne({_id: myID})))
            }
        }
    }




    function oltpInsert(mydb, num_collections, num_docs_per_collection) {
        
        var oltp_inserts = 2

        var coll = randomCollection(mydb, num_collections)

        for (var j=0; j < oltp_inserts; j++) {
            var myID = Math.round(random() * (num_docs_per_collection - 1))

            var removeResult = coll.remove({_id: myID})
            if (removeResult == null) {
                print('oltp: remove operation failed')
            }
            else if (removeResult.nRemoved != 1) {
                print('oltp: could not remove document with _id', myID, 'in collection', coll)
                print(tojson(removeResult))
            }

            var k = Math.round(random()*10000000)
            var c = sysbenchString()
            var pad = '' + Math.round(random()*100000000000)
            for (var y = 0; y < 4; y++)
                pad += '-' + Math.round(random()*100000000000)
            var d = { _id: myID, k: k, c: c, pad: pad }

            var insertResult = coll.insert(d)
            if (insertResult == null) {
                print('oltp: insert operation failed')
            }
            else if (insertResult.nInserted != 1) {
                print('oltp: could not insert document with _id', myID, 'in collection', coll)
                print(tojson(insertResult))
            }

        }
    }


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



function simulate_sysbench_execute(num_workload_threads, num_collections, num_docs_per_collection) {
    // workload execute phase
    var threads = []
    print('\nstarting', num_workload_threads, 'threads')

    var t = Math.round(Date.now() / (num_workload_threads + 1))
    for (var i = 0; i < num_workload_threads; i++) {
        threads[i] = new ScopedThread(workload, num_collections, num_docs_per_collection, (i + 1) * t)
        threads[i].start()
    }

    print("all threads started")
    threads.forEach(function(t) {
        t.join();
    });
    threads = []
}



 



