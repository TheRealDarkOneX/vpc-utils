# Virtual Private Cloud Utilities

Utilities for building, working with, and managing virtual private clouds.

## Job Scheduler

This utility has several components that interact with a RabbitMQ server to
perform offline, asynchronous tasks.

Common to all types of handlers is the security key that must exist in the
message headers before the handler does any work. The key must be named
"security.key" and can be anything. I use a UUID that I generate from the
*NIX "uuid" command-line tool. You can use the string "Bob" if you wanted.

All handlers respond to the ReplyTo feature of AMQP. If you expect a response,
then just set a replyTo value in your message properties.

Securing these handlers is the responsibility of your application. They are
trivially secured by using a security token that must be encoded into your
message headers. These utilities are meant to be used in a private cloud
environment, so there are different security expectations for that than maybe
what you're used to.

#### Building:

Edit the pom.xml to include any database drivers you might want to use and/or
exclude the PostgreSQL and AS/400 (JTOpen) drivers I use.

<pre><code>git clone git://github.com/jbrisbin/vpc-utils.git
cd vpc-utils/job-scheduler
mvn compile
mkdir lib
mvn dependency:copy-dependencies -DoutputDirectory=lib/
</code></pre>

Edit any pertinent settings in "etc/jobsched.xml", "etc/common.xml", and
"etc/jobsched.properties".

#### Running:

<pre><code>bin/run com.jbrisbin.vpc.jobsched.JobScheduler
</code></pre>

#### Testing:

If you want to do interactive testing using the Groovy shell, edit the script
"bin/groovysh" and give it the full path to your groovysh script. Then run:

<pre><code>bin/groovysh
</code></pre>

This will start the groovy shell with all the dependencies in the classpath.

### EXE Handler
***

This message handler expects the following JSON data as the body of the message:

<pre><code>{
	dir: "/path/to/working/dir",
	exe: "/path/to/program.sh",
	env: {
		"PATH": "/bin:/sbin:/usr/bin:/usr/sbin",
		"MYPROG_HOME": "/path/to/my/program_home"
	},
	args: [
		"-l",
		"-v",
		"-f",
		"/etc/my/config"
	]
}
</code></pre>

Pretty self-explanatory, but basically you can set the full environment, working
directory, full or relative path to your EXE, Bash script, or executable, and a
list of arguments.

The standard output of the program will be captured and returned as-is. JSON data
goes in, plain data (or binary data) comes out. No encoding is attempted on the
data coming out. It simply passes the data byte-for-byte back to the requestor.

### Batch Handler
***

This message handler expects a zip file as the body of the message. The zip file
should contain text files that are the bodies of messages you would otherwise
send individually. The names of the zip entries should be encoded as such:

<pre><code>exchange:routingKey:correlationId:securityKey
</code></pre>

The name of the zip entry will be split on the colon, so don't use a colon for
anything in your correlationId or securityKey.

You can't send custom headers, unfortunately, as there's no real place to stick
that data unless it's confusingly encoded into the batch message headers by pre-fixing
them with the correlationId. I've not implemented that, but would be happy to accept
patches for said feature. ;)

### SQL Handler
***

This message handler executes SQL encoded within JSON messages like the following:

<pre><code>{
	datasource: "spring-managed-ds-bean",
	sql: "SELECT * FROM MYSCHEMA.MYTABLE WHERE id=?",
	start: 50,
	limit: 10,
	params: [
		12345
	]
}</code></pre>

This utility can be hugely useful, assuming you trust your producers.

The response will be JSON data as well:

<pre><code>{
	totalRows: 1,
	columnNames: ["id", "desc"],
	errors: null,
	data: [
		[12345, "this is the description"]
	]
}
</code></pre>

There is also a swiss army knife for these message types in that you can extend
the functionality of the SQL handler by writing Groovy plugins. To invoke a plugin,
reference it like the following in your "sql" setting:

<pre><code>  ...
	sql: "plugin:my/groovy/plugin.groovy?param=paramValue&param2=paramValue"
	...
</code></pre>

Your plugin is expected to provide three properties the SQL handler needs to function:

sql = Closure | String

To get the actual SQL statement to prepare, provide a String or a Closure to run. If
you provide a Closure, the message object will be passed as the only parameter. That
object is an instance of com.jbrisbin.vpc.jobsched.sql.SqlMessage.

URL-style parameters can be passed to your plugin but they are not automatically parsed.
Your plugin is responsible for extracting these parameters. Pass the plugin string from
"message.sql" to something like this:

<pre><code>def parseParams(qs) {
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
</code></pre>

statementCreator = Closure

This closure will be called and passed the connection and the message. Something like
this should work:

<pre><code>statementCreator = { conn, msg ->
  CallableStatement stmt = conn.prepareCall(msg.sql)
  if (msg.params) {
    index = 1
    msg.params.each { o ->
      stmt.setObject(index++, o)
    }
  }
  stmt
}
</code></pre>

callback = Closure

This closure will be called from a CallableStatementCallback and passed the CallableStatement
and the message:

<pre><code>callback = { stmt, msg ->
	...
}
</code></pre>

Plugins are responsible for executing their SQL. Nothing will happen if, within your callback,
you don't do a "stmt.execute()" or "stmt.query()".

### Map/Reduce Handler
***

The Map/Reduce implementation within this job scheduler is built around Groovy. It is not
complete yet. I'm still working on getting things coordinated so that results are properly
routed back to the requestor.

### GroovyZooKeeperHelper
***

Since I'm using ZooKeeper to coordinate the Map/Reduce framework, I've written a helper class
that should be able to be used by any Groovy code to make interacting with ZooKeeper a little
more Groovy. Here's an example of how make working with ZooKeeper easier within a Groovy
script:

<pre><code>import com.jbrisbin.vpc.jobsched.zk.GroovyZooKeeperHelper

zk = new GroovyZooKeeperHelper("localhost:2181",
    onNodeChildrenChanged: { evt ->
      println "childrenChanged: ${evt}"
    },
    onDataChanged: { evt ->
      println "data changed: ${evt}"
    },
    onEvent: { evt ->
      println "none: ${evt}"
    }
)

jobs = "/vpc/mapred/jobs"

// Make sure parent exists
if (!zk.exists(jobs)) {
  zk.createPersistentNodeAndParents(jobs)
}

// Create node for job run
jobId = "test"
jobNode = zk.createSequenceNode("${jobs}/${jobId}.")

// Test setting integer
jobNode.data = 1
println "jobNode: ${jobNode.data}"

// Test setting string
jobNode.data = "String"
println "jobNode: ${jobNode.data}"
</code></pre>

The thing I like about this versus using the API directly is that I'm using Closures
everywhere I can. I've implemented some core Watchers that call the closures you set on
the "zk" object (specified in the example as a Map on the constructor). All methods
also take either a plain String or GString, so you can use the full power of Groovy strings
(including embedding variable references for building up paths).

There is a small wrapper class for dealing with a Node directly, rather than by calling
getData() and setData() (though you're certainly free to call those methods directly).

Automatic serialization and deserialization occur when setting and getting data.
You don't need to worry about it yourself. Any Serializable class can be set as the data on
a ZooKeeper node.