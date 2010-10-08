zk = new com.jbrisbin.vpc.zk.GroovyZooKeeperHelper("localhost:2181",
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
for (i in 1..5) {
  node = zk.createSequenceNode("${jobs}/${jobId}.")

  // Test setting integer
  node.data = i
  println "setting integer on node: ${node.data}"
}

zk.getChildren(jobs).each {
  path = "${jobs}/${it}"
  data = zk.getData(path)
  println "child: ${it}=${data}"
  zk.delete path
}

zk.delete "/vpc/mapred/jobs"



