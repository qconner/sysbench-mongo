'use strict';

// declare ScopedThread() function from jstests/core/parallelTester.js
load('parallelTester.js');


// declare our data load and transaction mix execution functions
load('load.js')
load('execute.js')

// define collection count
// n.b. one thread per collection will be spawned to load data
var collection_count = 16


// define number of documents per collection
//   documents are ~256 bytes so consider this
//   relative to the amount of DRAM available
var docs_per_collection = 20000000
//docs_per_collection = 80000


// define number of threads to spawn for the execute phase
//   should be >= number of cpu cores for a good workout
var thread_count = 64
//thread_count = 7


// begin the workload noting that data load can take some time
// depending on the working set size

for (var i=0; i < 100; i++) {

    // load data
    simulate_sysbench_load(collection_count, docs_per_collection)

    for (var j=0; j < 5; j++) {
        // execute a mix of read and write operations
        simulate_sysbench_execute(thread_count, collection_count, docs_per_collection)
    }

    // TODO: elapsed time-based execute phase

}

