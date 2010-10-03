import com.jbrisbin.vpc.jobsched.zk.GroovyZooKeeperHelper

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




