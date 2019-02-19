# Web Front-end for Tornado
## Paper: 
Tornado: A Distributed Spatio-Textual Stream Processing System
Ahmed Mahmood (Purdue University), Ahmed Aly (Purdue University), Thamir Qadah (Purdue University), El Kindi Rezig (Purdue University), Anas Daghistani (Purdue University), Amgad Madkour (Purdue University), Ahmed Abdelhamid (Purdue University), Mohamed Hassan (Purdue University), Walid Aref (Purdue University (USA), Saleh Basalamah (Umm Al-Qura University)

### Please cite our paper if you used this code.

## Build from Souce
### Mac/Linux
The project comes with an sbt launcher script. With this script, you don't need to have sbt installed. First, clone the git repo, using the following command: 
```sh
$ git clone https://github.com/qadahtm/webui/tree/tornado-ui
```
Next, build the project using:
```sh
$ ./sbt pack
```
Scripts for launching the executables will be available in `./target/pack/bin` directory.

To run the webserver: 
```sh
$ ./target/pack/bin/TornadoUI
```

In the browser, open `http://localhost:9990/newui.htm`


## Setting up eclipse
The project is already configured to generate Eclipse project files in order to utilize Eclipse as the IDE.

Simply, run 
```sh
$ ./sbt eclipse
```

Then import project from the Eclipse menu (i.e. File -> Import...)


## How to consume queries in Kafka
 - In TornadoUI/application.conf, Set enabled = true for kafka
 - Submit queries from UI and consume in kafka topic queries
 - Use port 9092 to produce and consume messages 
 - Create topics "output" and "queries"