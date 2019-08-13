package pl.allegro.tech.hermes.consumers.supervisor.workload.constraints;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.ArrayUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperBasedRepository;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;

import java.util.HashMap;

public class ZookeeperWorkloadConstraintsRepository extends ZookeeperBasedRepository implements WorkloadConstraintsRepository {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperWorkloadConstraintsRepository.class);

    public ZookeeperWorkloadConstraintsRepository(CuratorFramework zookeeper, ObjectMapper mapper, ZookeeperPaths paths) {
        super(zookeeper, mapper, paths);
    }

    @Override
    public ConsumersWorkloadConstraints getConsumersWorkloadConstraints() {
        ConsumersWorkloadConstraints workloadConstraints = new ConsumersWorkloadConstraints(new HashMap<>(), new HashMap<>());
        try {
            zookeeper.getChildren()
                    .forPath(paths.consumersWorkloadConstraintsPath())
                    .forEach(childrenPath -> {
                        String nodePath = String.format("%s/%s", paths.consumersWorkloadConstraintsPath(), childrenPath);
                        try {
                            final byte[] data = zookeeper.getData().forPath(nodePath);
                            if (ArrayUtils.isNotEmpty(data)) {
                                final Constraints constraints = mapper.readValue(data, Constraints.class);
                                if (childrenPath.contains("$")) {
                                    workloadConstraints.getSubscriptionConstraints()
                                            .put(SubscriptionName.fromString(childrenPath), constraints);
                                } else {
                                    workloadConstraints.getTopicConstraints()
                                            .put(TopicName.fromQualifiedName(childrenPath), constraints);
                                }
                            }
                        } catch (Exception e) {
                            logger.warn("Error while reading data from node {}", nodePath, e);
                        }
                    });
        } catch (Exception e) {
            logger.warn("Error while reading path {}", paths.consumersWorkloadConstraintsPath(), e);
        }
        return workloadConstraints;
    }
}
