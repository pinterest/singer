## <img src="../docs/icons/icon-singer-sk-small.png" alt="Singer logo" width="22" align="bottom"> &nbsp; Singer Tutorial

This tutorial is to demo locally on how Singer works. 
In this tutorial, we use Singer to upload
data in [$SINGER_HOME/tutorial/sample_data](sample_data) to 
a local Kafka broker, and then tail the local kafka topics to see the uploaded
messages and Singer heartbeats. The related Singer configurations
are at [$SINGER_HOME/tutorial/etc/singer](etc/singer).


#### Step 1: Build Singer binary

Run the following command under Singer home directory: 

```bash
mvn clean package -pl singer -am -DskipTests
```

#### Step 2: Run `tutorial_quickstart.sh` 

[tutorial_quickstart.sh](tutorial_quickstart.sh) downloads kafka binary, 
start a local zookeeper process and a kafka process, 
create kafka topics for this tutorial, 
and start Singer process. 

```bash
$SINGER_HOME/tutorial/tutorial_quickstart.sh
```

#### Step 3: View the messages that Singer uploads 

[tutorial_kafka_tailer.sh](tutorial_kafka_tailer.sh) shows the messages that uploaded by Singer to the local 
Kafka broker. 

```bash
$SINGER_HOME/tutorial/tutorial_kafka_tailer.sh
```


#### Step 4: View Singer Heartbeat messages

[tutorial_singer_heartbeats.sh](tutorial_singer_heartbeats.sh) allows us to view
the heartbeat messages that Singer sends to Kafka. In this tutorial, we set the heartbeat 
interval as 10 seconds. 

```bash
$SINGER_HOME/tutorial/tutorial_singer_heartbeats.sh
```

Singer heartbeat messages can be used for central Singer monitoring,
alerting, and visualization. 


