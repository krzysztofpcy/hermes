package pl.allegro.tech.hermes.consumers.supervisor.workload.constraints;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import pl.allegro.tech.hermes.test.helper.zookeeper.ZookeeperBaseTest;

import java.util.List;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class ZookeeperPathChildrenCacheTest extends ZookeeperBaseTest {

    private ZookeeperPathChildrenCache pathChildrenCache;

    @Before
    public void setUp() {
        pathChildrenCache = new ZookeeperPathChildrenCache(zookeeperClient, 1);
    }

    @Test(expected = KeeperException.NoNodeException.class)
    public void shouldReturnEmptyListIfBasePathDoesNotExistTest() throws Exception {
        pathChildrenCache.getChildrenPaths("/hermes/consumers-workload-constraints");
    }

    @Test
    public void shouldReturnListOfChildrenPathsTest() throws Exception {
        // given
        createPath("/hermes/consumers-workload-constraints/group.topic");
        createPath("/hermes/consumers-workload-constraints/group.topic$sub");

        // when
        List<String> childrenPaths = pathChildrenCache.getChildrenPaths("/hermes/consumers-workload-constraints");

        // then
        assertThat(childrenPaths).contains("group.topic", "group.topic$sub");
        assertThat(childrenPaths).hasSize(2);
    }
}
