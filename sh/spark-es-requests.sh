

spark.conf.set("spark.es.nodes", "172.21.173.198")
spark.conf.set("spark.es.port", "9200")
spark.conf.set("spark.es.net.http.auth.user", "elastic")
spark.conf.set("spark.es.net.http.auth.pass", "orange")
spark.conf.set("spark.es.nodes.wan.only", "true")
spark.conf.set("spark.es.net.ssl", "true")
spark.conf.set("spark.es.net.ssl.cert.allow.self.signed", "true")
spark.conf.set("spark.es.net.ssl.keystore.location","file:///etc/elasticsearch/certs/quicksearch-mnode-1.p12")
spark.conf.set("spark.es.net.ssl.truststore.location","file:///etc/elasticsearch/certs/quicksearch-mnode-1.p12")
spark.conf.set("es.net.ssl.truststore.pass","changeit")

.config("spark.es.net.ssl", "true")
.config("spark.es.net.ssl.cert.allow.self.signed", "true")

curl -X GET "https://172.21.173.198:9200/_cluster/health?pretty" -u "elastic:orange" -H "Content-Type: application/json" --insecure

curl -X GET "https://172.21.173.198:9200/_cluster/health?pretty" -u "username:password" -H "Content-Type: application/json" --insecure

openssl pkcs12 -in /path/to/your/certificate.p12 -out certificate.pem -nodes
curl -X GET "https://172.21.173.198:9200/_cluster/health?pretty" -u "elastic:orange" -H "Content-Type: application/json" --cacert certificate.pem

spark-shell \
  --jars /jar/elasticsearch-spark-20_2.11-7.5.0.jar \
  --conf spark.es.nodes=172.21.173.198 \
  --conf spark.es.port=9200 \
  --conf spark.es.net.http.auth.user=elastic \
  --conf spark.es.net.http.auth.pass=orange \
  --conf spark.es.net.ssl=true \
  --conf spark.es.net.ssl.cert.allow.self.signed=false \
  --conf spark.es.net.ssl.truststore.location=file:///elasticsearch-truststore.jks \
  --conf spark.es.net.ssl.truststore.type=JKS \
  --conf spark.es.net.ssl.truststore.pass=changeit

  spark-shell \
    --jars /jar/elasticsearch-spark-30_2.12-7.12.0.jar \
    --conf spark.es.nodes=172.21.173.198 \
    --conf spark.es.port=9200 \
    --conf spark.es.net.http.auth.user=elastic \
    --conf spark.es.net.http.auth.pass=orange \
    --conf spark.es.net.ssl=true \
    --conf spark.es.net.ssl.cert.allow.self.signed=false \
    --conf spark.es.net.ssl.truststore.location=file:///conf/elasticsearch-truststore.jks \
    --conf spark.es.net.ssl.truststore.type=JKS \
    --conf spark.es.net.ssl.truststore.pass=changeit

    --jars /jar/elasticsearch-spark-30_2.12-7.12.0.jar,/jar/commons-httpclient-3.1.jar     --conf spark.es.nodes=172.21.173.198     --conf spark.es.port=9200     --conf spark.es.net.http.auth.user=elastic     --conf spark.es.net.http.auth.pass=orange     --conf spark.es.net.ssl=true     --conf spark.es.net.ssl.cert.allow.self.signed=false     --conf spark.es.net.ssl.truststore.location=file:///conf/elasticsearch-truststore.jks     --conf spark.es.net.ssl.truststore.type=JKS     --conf spark.es.net.ssl.truststore.pass=changeit


spark-shell     --jars /jar/elasticsearch-spark-30_2.12-7.12.0.jar,/jar/commons-httpclient-3.1.jar,/jar/jsch-0.2.17.jar     --conf spark.es.nodes=172.21.173.198     --conf spark.es.port=9200     --conf spark.es.net.http.auth.user=elastic     --conf spark.es.net.http.auth.pass=orange     --conf spark.es.net.ssl=true     --conf spark.es.net.ssl.cert.allow.self.signed=false     --conf spark.es.net.ssl.truststore.location=file:///conf/elasticsearch-truststore.jks     --conf spark.es.net.ssl.truststore.type=JKS     --conf spark.es.net.ssl.truststore.pass=changeit


@
//------------------------------------------
import com.jcraft.jsch.{JSch, ChannelSftp}
import scala.collection.JavaConverters._

var filepath_df = spark.read.parquet("file:///data/telco-files.parquet")

val filepaths = filepath_df.select("filepath").limit(3).collect().map(_.getString(0))

val host = "172.21.14.254"
val username = "datalab"
val password = "Or0nget2@2*00"
val port = 22

val jsch = new JSch()
val session = jsch.getSession(username, host, port)
session.setPassword(password)
session.setConfig("StrictHostKeyChecking", "no")
session.connect()

val channel = session.openChannel("sftp").asInstanceOf[ChannelSftp]
channel.connect()
//------------------------------------------

filepaths.foreach { filepath =>
  val localPath = s"/data/${new java.io.File(filepath).getName}"
  channel.get(filepath, localPath)
}
//------------------------------------------

filepaths.foreach { filepath =>
  val localPath = s"/data/${new java.io.File(filepath).getName}"
  println(s"Transferring: $filepath to $localPath")
  channel.get(filepath, localPath)
  println(s"Transfer complete: $filepath")
}
//------------------------------------------

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

filepaths.foreach { filepath =>
  val localPath = s"/data/${new java.io.File(filepath).getName}"
  val timestamp = LocalDateTime.now().format(formatter)
  println(s"[$timestamp] Starting transfer: $filepath to $localPath")

  val fileSize = channel.lstat(filepath).getSize
  val progress = new SftpProgressMonitor {
    private var transferred: Long = 0
    override def init(op: Int, src: String, dest: String, max: Long): Unit = {}
    override def count(count: Long): Boolean = {
      transferred += count
      val percent = (transferred.toDouble / fileSize * 100).toInt
      println(s"[$timestamp] Progress: $percent%")
      true
    }
    override def end(): Unit = {}
  }

  channel.get(filepath, localPath, progress)
  println(s"[$timestamp] Transfer complete: $filepath")
}
//------------------------------------------

def ungzip(gzipFile: String): Unit = {
  val outputFile = gzipFile.replaceAll("\\.gz$", "")

  Using.resources(
    new GZIPInputStream(new FileInputStream(gzipFile)),
    new FileOutputStream(outputFile)
  ) { (in, out) =>
    val buffer = new Array[Byte](1024)
    var len = in.read(buffer)
    while (len > 0) {
      out.write(buffer, 0, len)
      len = in.read(buffer)
    }
  }

  println(s"Unzipped: $gzipFile to $outputFile")
}

val filepaths = List(
  "/dev/data/RL_CRA_02062023_00000000000000.csv.gz",
  "/dev/data/RL_CRA_02062023_00000000000001.csv.gz",
  "/dev/data/RL_CRA_02062023_00000000000002.csv.gz",
)


channel.disconnect()
session.disconnect()

//------------------------------------------

val data = Seq((1, "Alice", 28), (2, "Bob", 35), (3, "Charlie", 42)).toDF("id", "name", "age")
data.saveToEs("a-kplr-spark3/doc")

//------------------------------------------

from pyspark.sql.types import *

data = [(1, "Alice", 28), (2, "Bob", 35), (3, "Charlie", 42)]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False)
])

df = spark.createDataFrame(data, schema)

df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.resource", "a-kplr-spark-python/doc") \
    .mode("overwrite") \
    .save()

//------------------------------------------

data = [(1, "Alice", 28), (2, "Bob", 35), (3, "Charlie", 42)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.write.format("org.elasticsearch.spark.sql").option("es.resource", "a-kplr-spark-python/doc").mode("overwrite").save()

elasticsearch-truststore.jks
println(spark.conf.get("spark.es.net.ssl.truststore.path"))
println(spark.conf.get("spark.es.port"))

import org.elasticsearch.spark.sql._
val df = spark.read.format("es").load("_all")

import org.elasticsearch.spark.sql._
val data = Seq((1, "Alice", 28), (2, "Bob", 35), (3, "Charlie", 42)).toDF("id", "name", "age")
data.saveToEs("a-kplr-spark3/doc")

println("Data written to dummy_index")

spark-shell \
  --conf "spark.es.nodes=https://172.21.173.198" \
  --conf "spark.es.port=9200" \
  --conf "spark.es.net.http.auth.user=elastic" \
  --conf "spark.es.net.http.auth.pass=orange" \
  --conf "spark.es.nodes.wan.only=true" \
  --conf "spark.es.net.ssl=true" \
  --conf "spark.es.net.ssl.cert.allow.self.signed=true" \
  --conf "spark.es.net.ssl.hostname.verification=false" \
  --jars /tmp/elasticsearch-spark-20_2.11-7.5.0.jar

          .option("es.net.http.auth.user", username) \
          .option("es.net.http.auth.pass", password) \
          .option("es.net.ssl", "true") \
          .option("es.net.ssl.cert.allow.self.signed", "true") \
          .option("mergeSchema", "true") \
          .option('es.index.auto.create', 'true') \
          .option('es.nodes', 'https://{}'.format(es_ip)) \
          .option('es.port', '9200') \
          .option('es.batch.write.retry.wait', '100s') \
          .save('{index}/_doc'.format(index=index))

  --conf "spark.es.net.ssl.keystore.location=file:///tmp/quicksearch-mnode-1.p12" \
  --conf "spark.es.net.ssl.truststore.location=file://tmp/quicksearch-mnode-1.p12" \

import org.elasticsearch.spark.sql._
val df = spark.read.format("es").load("_all")

import org.elasticsearch.spark.sql._
val df = spark.read.format("es").option("es.net.ssl", "true").option("es.net.ssl.cert.allow.self.signed", "true").option("es.net.ssl.hostname.verification", "false").load("_all")

import org.elasticsearch.spark.sql._
val df = spark.read.format("org.elasticsearch.spark.sql").option("es.net.http.auth.user", "elastic").option("es.net.http.auth.pass", "orange").option("es.net.ssl", "true").option("es.net.ssl.cert.allow.self.signed", "true").option("mergeSchema", "true").option("es.index.auto.create", "true").option("es.nodes","https://172.21.173.198").option("es.port", "443").option("es.batch.write.retry.wait", "100").load("_all")

spark.conf.set("spark.es.nodes", "172.21.173.198")
spark.conf.set("spark.es.port", "9200")
spark.conf.set("spark.es.net.http.auth.user", "user")
spark.conf.set("spark.es.net.http.auth.pass", "password")
spark.conf.set("spark.es.nodes.wan.only", "true")
spark.conf.set("spark.es.net.ssl", "true")
spark.conf.set("spark.es.net.ssl.cert.allow.self.signed", "true")

println(spark.conf.get("spark.es.nodes"))
println(spark.conf.get("spark.es.port"))
