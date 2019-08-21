package pl.allegro.tech.hermes.consumers.supervisor.workload.constraints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperBasedRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang.ArrayUtils.isNotEmpty;

public class ZookeeperWorkloadConstraintsRepository extends ZookeeperBasedRepository implements WorkloadConstraintsRepository {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperWorkloadConstraintsRepository.class);

    private final ZookeeperPathChildrenCache pathChildrenCache;

    public ZookeeperWorkloadConstraintsRepository(CuratorFramework zookeeper, ObjectMapper mapper, ZookeeperPaths paths,
                                                  int pathsCacheExpireAfterMinutes) {
        super(zookeeper, mapper, paths);
        this.pathChildrenCache = new ZookeeperPathChildrenCache(zookeeper, pathsCacheExpireAfterMinutes);
    }

    @Override
    public ConsumersWorkloadConstraints getConsumersWorkloadConstraints() {
        try {
            final Map<TopicName, Constraints> topicConstraints = new HashMap<>();
            final Map<SubscriptionName, Constraints> subscriptionConstraints = new HashMap<>();
            pathChildrenCache.getChildrenPaths(paths.consumersWorkloadConstraintsPath())
                    .forEach(childrenPath -> {
                        String nodePath = String.format("%s/%s", paths.consumersWorkloadConstraintsPath(), childrenPath);
                        try {
                            final byte[] data = zookeeper.getData().forPath(nodePath);
                            if (isNotEmpty(data)) {
                                final Constraints constraints = mapper.readValue(data, Constraints.class);
                                if (isSubscription(childrenPath)) {
                                    subscriptionConstraints.put(SubscriptionName.fromString(childrenPath), constraints);
                                } else {
                                    topicConstraints.put(TopicName.fromQualifiedName(childrenPath), constraints);
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("Error while reading data from node {}", nodePath, e);
                        }
                    });
            return new ConsumersWorkloadConstraints(topicConstraints, subscriptionConstraints);
        } catch (Exception e) {
            logger.warn("Error while reading path {}", paths.consumersWorkloadConstraintsPath(), e);
            return new ConsumersWorkloadConstraints(emptyMap(), emptyMap());
        }
    }

    private boolean isSubscription(String path) {
        return path.contains("$");
    }
}
