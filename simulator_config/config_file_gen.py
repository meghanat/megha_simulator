import os
import json

num_nodes = 1000
num_lms = 10
num_partitions = 10  # This value is always equal to the number of GMs

assert num_nodes % (num_lms * num_partitions) == 0, ("Nodes cannot be equally "
                                                     "divided amongst all "
                                                     "partitions")
partition_size = num_nodes // (num_lms * num_partitions)

cluster_config = {}
cluster_config["LMs"] = {}

for i in range(1, num_lms + 1):
    lm_id = str(i)
    cluster_config["LMs"][lm_id] = {"LM_id": str(i), "partitions": {}}
    part_dict = {}
    for j in range(1, num_partitions + 1):
        part_dict[str(j)] = {"partition_id": str(j), "nodes": {}}

        nodes_dict = {}
        for k in range(1, partition_size + 1):
            nodes_dict[str(k)] = {"CPU": 1, "RAM": 1,
                                  "DISK": 1, "constraints": []}

        part_dict[str(j)]["nodes"] = nodes_dict

    cluster_config["LMs"][lm_id]["partitions"] = part_dict

string = json.dumps(cluster_config, indent=4)

if len(string) <= 500:
    print(string)
    print()
    print("This output has been added to the file 'config.json' as well, "
          "in the 'simulator_config' folder.")
else:
    print("Output too long! directly writing output to the file"
          " 'config.json' in the 'simulator_config' folder.")

config_file_path = os.path.join("./simulator_config/config.json")


def write_config_file(string: str, config_file_path: str):
    """
    Write the generated configuration to the file.

    Args:
        string (str): The string containing the generated configuration.
        config_file_path (str): Path to the config file to write into.
    """
    with open(config_file_path, "w") as file_handler:
        file_handler.write(f"{string}\n")


if os.path.exists(config_file_path):
    print("This file already exists, maybe with another configuration...")
    print("Do you want to overwrite it? [y/n]")
    user_inp = None
    while user_inp != "n" and user_inp != "y":
        user_inp = input().strip().lower()
        if user_inp == "n":
            print("Goodbye!")
            exit()
        elif user_inp == "y":
            write_config_file(string, config_file_path)
            print("Done, the output has been written out, into the file!")
        else:
            print("Do you want to overwrite it? [y/n]")
else:
    write_config_file(string, config_file_path)
    print("Done, the output has been written out, into the file!")
