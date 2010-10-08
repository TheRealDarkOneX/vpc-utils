import com.jbrisbin.vpc.zk.GroovyZooKeeperHelper
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TestZkHelper {

  Logger log = LoggerFactory.getLogger(getClass())
  def zk

  @Before
  void before() {
    zk = new GroovyZooKeeperHelper("localhost:2181",
        onEvent: { evt ->
          log.info "ZooKeeper event: ${evt}"
        })
  }

  @Test
  void testCreate() {

    // Node shouldn't exist before tests
    assert null == zk.exists("/zkhelpertest")
    def node = zk.create("/zkhelpertest", "test data")

    assert "test data" == node.data
  }

  @After
  void after() {
    zk.delete("/zkhelpertest")
    zk.close()
  }


}