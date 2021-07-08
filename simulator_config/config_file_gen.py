import json

num_nodes = 10_000
num_lms = 10
num_partitions = 10  # This value is always equal to the number of GMs

assert num_nodes % (num_lms * num_partitions) == 0, ("Nodes cannot be equally "
                                                     "divided amongst all "
                                                     "partitions")
partition_size = num_nodes // (num_lms * num_partitions)

cluster_config = {}
cluster_config["LMs"] = {}

for i in range(1, num_lms+1):
    lm_id = str(i)
    cluster_config["LMs"][lm_id] = {"LM_id": str(i), "partitions": {}}
    part_dict = {}
    for j in range(1, num_partitions+1):
        part_dict[str(j)] = {"partition_id": str(j), "nodes": {}}

        nodes_dict = {}
        for k in range(1, partition_size+1):
            nodes_dict[str(k)] = {"CPU": 1, "RAM": 1,
                                  "DISK": 1, "constraints": []}

        part_dict[str(j)]["nodes"] = nodes_dict

    cluster_config["LMs"][lm_id]["partitions"] = part_dict

string = json.dumps(cluster_config, indent=4)
print(string)
