import java.sql.CallableStatement
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.util.concurrent.TimeUnit
import org.codehaus.jackson.map.ObjectMapper
import org.springframework.amqp.core.Message
import org.springframework.amqp.core.MessageCreator
import org.springframework.amqp.core.MessageProperties
import org.springframework.amqp.rabbit.core.RabbitMessageProperties
import org.springframework.amqp.rabbit.core.RabbitTemplate

params = [:]
ObjectMapper mapper = new ObjectMapper()
startTime = System.currentTimeMillis()
endTime = System.currentTimeMillis()

statementCreator = { conn, msg ->
  CallableStatement stmt = conn.prepareCall(msg.sql)
  if (msg.params) {
    index = 1
    msg.params.each { o ->
      stmt.setObject(index++, o)
    }
  }

  stmt
}

callback = { CallableStatement stmt, msg ->
  msg.results.data = []
  msg.results.columnNames = ["key", "total"]
  String lastKey = null
  zk = new com.jbrisbin.vpc.zk.GroovyZooKeeperHelper("localhost:2181",
      onNodeChildrenChanged: { evt ->
      },
      onEvent: { evt ->

      }
  )

  listener = listen({ result, props ->
    log.debug("got result: ${result}")
    endTime = System.currentTimeMillis()
    [result.key, result.total]
  })
  replyTo = listener.queueName

  jobNode = zk.createPersistentNodeAndParents("/vpc/mapred/jobs/${msg.id}")
  if (stmt.execute()) {
    ResultSet results = stmt.resultSet
    ResultSetMetaData meta
    columnNames = []
    data = []

    while (results.next()) {
      // Populate meta data
      if (!meta) {
        columnNames.add(null)
        meta = results.metaData
        for (i in 1..meta.columnCount) {
          columnNames.add(meta.getColumnName(i))
        }
      }

      // Data
      key = results.getString(1)
      row = [:]
      for (i in 2..meta.columnCount) {
        row[columnNames[i]] = results.getString(i)
      }

      ByteArrayOutputStream out = new ByteArrayOutputStream()
      mapper.writeValue out, row

      // Map/Reduce
      mapreduce(message.id, replyTo, params["src"], key, row)

      s = new String(out.toByteArray())
      log.debug "lastKey: ${lastKey}, key: ${key}, val: ${s}"
    }

    results.close()
  }

  while ((row = listener.results.poll(1, TimeUnit.SECONDS))) {
    msg.results.data << row
  }
  totalTime = endTime - startTime
  println "run time: ${totalTime}"

  null
}

sql = { msg ->
  String sqlst = msg.sql.substring(msg.sql.indexOf("?"))
  props = parseParams(sqlst)
  println "props: ${props}, params: ${msg.params}"
  props.sql
}

def parseParams(qs) {
  String[] pairs
  if (qs.startsWith("?")) {
    pairs = qs.substring(1).split("&")
  }
  pairs.each { p ->
    String[] kv = p.split("=")
    params[kv[0]] = URLDecoder.decode(kv[1], "utf-8")
  }
  params
}

def done() {
  log.debug(" ********* DONE!")
  RabbitTemplate tmpl = bean("rabbitTemplate")
  mc = [
      createMessage: {
        MessageProperties props = new RabbitMessageProperties()
        props.setContentType "application/json"
        props.setCorrelationId message.id.bytes
        Message msg = new Message("END".bytes, props)
        println "sending control msg: ${msg}"
        msg
      }
  ] as MessageCreator
  tmpl.send("mapred.control", null, mc)
}