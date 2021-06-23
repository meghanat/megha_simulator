# Megha Simulator

## About
A simple simulator for the Megha Federated Scheduling Framework. This simulator enables comparison with other frameworks such as Sparrow, Eagle and Pigeon for which simulators already exist.

## Setting up your Environment

1. Add the ```megha_sim``` folder to your ```PYTHONPATH``` environment variable by following steps:
   1. Open your terminal and in that, navigate into the ```megha_sim``` folder of the project
   2. Once in the ```megha_sim``` folder run the command
        ```bash
        $ pwd
        ```
        - This will give you the complete path to the ```megha_sim``` folder your are currently in.
        - Make sure to save this path returned, as it will be needed in the next steps
    1. Open your system's ```.bash_profile``` file using the command,
       ```bash
       $ nano ~/.bash_profile
       ```
       - Navigate to the bottom of the file
    2. Type in the following into the file,
        ```bash
        export PYTHONPATH="<The_path_returned_by_the_pwd_command_done_earlier>"
        ```
        - Save the file
        - Close the file
    3. Now run the below command on your terminal,
        ```bash
        $ source ~/.bash_profile
        ```
    4. Now run the below command on your terminal,
        ```bash
        $ echo $PYTHONPATH
        ```
        - You should see the path you had put as the value for the ```PYTHONPATH``` in the ```.bash_profile``` visible here
    5. **NOTE:** The same steps can also be done using the ```~/.bashrc``` file instead of the file ```~/.bash_profile```
       1. Using ```~/.bash_profile``` is recommended for systems running MacOS
       2. Any of the 2 files can used for Linux systems
    6. **NOTE:** You will need to ***refresh*** or ***restart*** your IDE after these steps, to get useful syntax highlighting and intellisense when working with the ```megha_sim``` module.

## Running the simulator

Sample command to run with 3 GMs and 2 LMs, and PARTITION_SIZE 3:

```bash
$ python3 ./src/runner.py ./traces/input/YH.tr ./simulator_config/config.json 3 2 3 1 1 1 
```

By convention, the output of the simulator, after running on a trace must always be saved in the folder `traces/output`. In our example, to save the output of the simulator, after running it on the trace `YH.tr`, into the folder `traces/output`, use the command:

```bash
$ python3 ./src/runner.py ./traces/input/YH.tr ./simulator_config/config.json 3 2 3 1 1 1 > traces/output/YH_OP.tr
```

**NOTE:** The general convention for naming the output of the simulator, after running it on the trace file is:

```
<Name_of_the_trace_file>_OP.tr
```

## Using the Development Environment Container

1. Make the shell script `enter_dev_env.sh` executable using the command:
```bash
$ sudo chmod u+x enter_dev_env.sh
```
2. Run the shell script `enter_dev_env.sh` using the command:
    ```bash
    $ ./enter_dev_env.sh
    ```
   1. This will automatically take you into the container with the project files visible in the current folder itself (try `ls`). As you update the code files in your host system they will be simultaneously updated in the container as well.
   1. Inside the container use Python by `python3`
   1. To 'persist' new module installations inside the container, update the requirements.txt file by running the following command from **inside the container**:
        ```bash
        pip3 freeze > requirements.txt
        ```
3. Once finished using the container, run the command `exit` inside the container to exit it.


## How to Generate the Documentation

1. Make the shell script `doc_gen.sh` executable using the command:
```bash
$ sudo chmod u+x doc_gen.sh
```
2. Run the shell script `doc_gen.sh` using the command:
    ```bash
    $ ./doc_gen.sh
    ```
    1. This will generate the documentation file called `megha_sim.html` in the folder `./html`
    2. Open the file using any modern web browser to view the documentation.

## Dependency Graph

![Project's Dependency Graph](./media/images/megha_sim_dep_graph.png)

## Link to the Paper

- [Link to the preprint on arXiv.org](https://arxiv.org/abs/2103.08413)
---