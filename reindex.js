const elasticsearch = require('elasticsearch');

const elasticClient = new elasticsearch.Client({
    host : "localhost:9300", 
    requestTimeout : 60000,
});

const source_index = "source_index";
const dest_index = "dest_index";

/**
 * reindex kill example
 * POST _tasks/node_id:task_number/_cancel
 */
(async () => {
    try {
        let reindex_body = {
            "source": {
                "index": source_index
            },
            "dest": {
                "index": dest_index
            }
        }

        let reindex_task = await elasticClient.reindex({
            body : reindex_body,
            wait_for_completion: false
        });

        let interval = await task_chek(reindex_task.task);

        if(interval === true){
            console.log("Success");
        }else if(interval === false){
            console.log("Error: interval false");
        }

    } catch (e) {
        console.log(e)
        process.exit();
    }
})();

const task_chek = async (task_id) => {
    
    await elasticClient.indices.refresh({index: ".tasks"});
    let task_search = await elasticClient.search({
        index: ".tasks",
        body: {
            "query": {
                "match": {
                    "_id": task_id
                }
            }
        }
    });
    

    let task_hits = task_search.hits.hits;
    let total = task_search.hits.total.value;
    if (total > 0) {
        let source = task_hits[0]._source;

        if (source.hasOwnProperty("error")) {
            console.log("Error:", JSON.stringify(source.error));
            return "false";
        }

        console.log("status:", source.task.status);
        console.log("description:", source.task.description);
    }
    console.log("start task check");
    return await waitInterval(task_id);
}

async function waitInterval(task_id) {

    let node_id = task_id.split(":")[0];

    return await new Promise( resolve => {
        const interval = setInterval(async () => {
           await check();
        }, 10000);

        const check = async () => {

            // reindex tasks search
            let task_search = await elasticClient.tasks.list({
                nodes: node_id,
                format: "json",
                detailed: true,
                actions: "*reindex"
            });
            
            if(!task_search.nodes.hasOwnProperty(node_id)){
                await elasticClient.indices.refresh({index: ".tasks"});

                let task_search = await elasticClient.search({
                    index: ".tasks",
                    body: {
                        "query": {
                            "match": {
                                "_id": task_id
                            }
                        }
                    }
                });

                let task_hits = task_search.hits.hits;
                let total = task_search.hits.total.value;
                if (total > 0) {
                    let source = task_hits[0]._source;
                    let task = source.task;

                    if (source.hasOwnProperty("error")) {
                        console.log("Error:", JSON.stringify(source.error));
                        resolve(false);
                    }

                    console.log(
                    ` -------------------------------- END -----------------------------------\n`,
                    "|", task.description, "\n",
                    "| running_second", task.running_time_in_nanos/1000000000, "\n",
                    "| total:", task.status.total, "\n",
                    "| updated:", task.status.updated, "\n",
                    "| created:", task.status.created, "\n",
                    "| deleted:", task.status.deleted, "\n",
                    "| batches:", task.status.batches, "\n",
                    "| version_conflicts:", task.status.version_conflicts, "\n",
                    " -------------------------------------------------------------------------------",
                    )
                }else{
                    console.log("task id:", task_id);
                    console.log("task_id가 조회되지 않습니다.");
                }
                clearInterval(interval);
                resolve(true);
                return;
            }

            if( !task_search.nodes.hasOwnProperty(node_id) || !task_search.nodes[node_id].hasOwnProperty("tasks")  
             || !task_search.nodes[node_id].tasks.hasOwnProperty(task_id))
            {
                console.log(`null: ${task_search}\n
                    node_id: ${node_id}\n
                    task_id: ${task_id}`
                )
                clearInterval(interval);
                resolve(false);
                return;
            }

            let tasks = task_search.nodes[node_id].tasks[task_id];
            let task_total = Object.keys(tasks);
            if(task_total > 0 ){
                console.log("task id가 존재하지 않습니다.");
                clearInterval(interval);
                resolve(false);
                return;
            }

            let total = 0;
            if(tasks.constructor === Object){
                total = 1;
            }else if(tasks.constructor === Array){
                total = tasks.length;
            }
            
            if(total > 1){
                console.log("같은 작업이 2개이상 진행중입니다.");
                for(let i in tasks){
                    console.log(`== ${tasks[i].id} ==`);
                    console.log(
                        `-----------------------------${tasks[i].id}-----------------------------------`,
                        "|", tasks.description, "\n",
                        "| running_second", tasks.running_time_in_nanos/1000000000, "\n",
                        "| total:", tasks[i].status.total, "\n",
                        "| updated:", tasks[i].status.updated, "\n",
                        "| created:", tasks[i].status.created, "\n",
                        "| deleted:", tasks[i].status.deleted, "\n",
                        "| batches:", tasks[i].status.batches, "\n",
                        "| version_conflicts:", tasks[i].status.version_conflicts,
                        "-----------------------------------------------------------------------------",
                    );
                }
            }else if (total > 0) {
                if (tasks.hasOwnProperty("error")) {
                    console.log("Error:", JSON.stringify(source.error));
                    clearInterval(interval);
                    resolve(false);
                    return;
                }
                console.log(
                    ` --------------------------------${tasks.id}-----------------------------------\n`,
                    "|", tasks.description, "\n",
                    "| running_second", tasks.running_time_in_nanos/1000000000, "\n",
                    "| total:", tasks.status.total, "\n",
                    "| updated:", tasks.status.updated, "\n",
                    "| created:", tasks.status.created, "\n",
                    "| deleted:", tasks.status.deleted, "\n",
                    "| batches:", tasks.status.batches, "\n",
                    "| version_conflicts:", tasks.status.version_conflicts, "\n",
                    " -------------------------------------------------------------------------------",
                );
            }
        }
    });
}