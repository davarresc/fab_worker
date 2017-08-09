<?php
require_once __DIR__ . '/vendor/autoload.php';
use PhpAmqpLib\Connection\AMQPStreamConnection;

//Variables
$HADOOP_HOME = "/usr/local/hadoop";
$HADOOP_ENDPOINT = "hdfs://10.141.10.111:9000/hdfs/fab/files";
$HADOOP_FOLDER = "/user/fab";
$FOLDER_UPLOAD = "/Library/WebServer/Documents/fab_app/backend/web/uploads";
$LOCATION_JAR = "/home/snape/fab_docker_server/applications/rateJob.jar";
$SPARK_ENDPOINT = "spark://debian:7077";

//Conexión RabbitQM
$connection = new AMQPStreamConnection('estigia.lsi.us.es', 11273, 'fab', 'fab_architecture');
$channel = $connection->channel();
$channel->queue_declare('fab_spark', false, true, false, false);
echo ' [*] Ejecutando worker....', "\n";
$callback = function($msg) use ($HADOOP_HOME, $HADOOP_ENDPOINT, $FOLDER_UPLOAD, $LOCATION_JAR, $SPARK_ENDPOINT){

	//Process
	echo " [x] Recibido ", $msg->body, "\n";
	$obj = json_decode($msg->body);
	echo $obj->filename;

	//Save into hdfs
	exec("$HADOOP_HOME/bin/hadoop fs -put $FOLDER_UPLOAD/".$obj->filename." $HADOOP_FOLDER");
	unlink("$FOLDER_UPLOAD/".$obj->filename);

	//Send to spark
	echo "spark-submit --master $SPARK_ENDPOINT --class=es.us.tfg.application.App --name ratejob $LOCATION_JAR $HADOOP_ENDPOINT/$obj->filename";
	exec("spark-submit --master $SPARK_ENDPOINT --class=es.us.tfg.application.App --name ratejob $LOCATION_JAR $HADOOP_ENDPOINT/$obj->filename");

	echo " [x] Procesado!", "\n";
	$msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
};

$channel->basic_qos(null, 1, null);
$channel->basic_consume('fab_spark', '', false, false, false, false, $callback);
while(count($channel->callbacks)) {
	$channel->wait();
}
$channel->close();
$connection->close();
?>