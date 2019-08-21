package pl.allegro.tech.hermes.consumers.supervisor.workload.constraints;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.infrastructure.zookeeper.ZookeeperPaths;
import pl.allegro.tech.hermes.test.helper.zookeeper.ZookeeperBaseTest;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Java6Assertions.assertThat;

public class ZookeeperWorkloadConstraintsRepositoryTest extends ZookeeperBaseTest {

    private ZookeeperWorkloadConstraintsRepository repository =
            new ZookeeperWorkloadConstraintsRepository(zookeeperClient, new ObjectMapper(), new ZookeeperPaths("/hermes"), 1);

    private Logger logger;
    private ListAppender<ILoggingEvent> listAppender;

    @Before
    public void setup() {
        logger = (Logger) LoggerFactory.getLogger(ZookeeperWorkloadConstraintsRepository.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        try {
            deleteAllNodes();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void shouldReturnEmptyConstraintsIfBasePathDoesNotExistTest() {
        // when
        final ConsumersWorkloadConstraints workloadConstraints = repository.getConsumersWorkloadConstraints();

        // then
        assertThat(workloadConstraints.getTopicConstraints()).isEqualTo(emptyMap());
        assertThat(workloadConstraints.getSubscriptionConstraints()).isEqualTo(emptyMap());

        // and
        assertThat(listAppender.list.get(0).getFormattedMessage())
                .isEqualTo("Error while reading path /hermes/consumers-workload-constraints");
        assertThat(listAppender.list.get(0).getThrowableProxy().getClassName())
                .isEqualTo("org.apache.zookeeper.KeeperException$NoNodeException");
        assertThat(listAppender.list.get(0).getLevel())
                .isEqualTo(Level.WARN);
    }

    @Test
    public void shouldReturnConstraintsForGivenTopicAndSubscriptionTest() throws Exception {
        // given
        TopicName topic = TopicName.fromQualifiedName("group.topic");
        SubscriptionName subscription = SubscriptionName.fromString("group.topic$sub");

        setupPath("/hermes/consumers-workload-constraints/group.topic", new Constraints(1));
        setupPath("/hermes/consumers-workload-constraints/group.topic$sub", new Constraints(3));

        // when
        ConsumersWorkloadConstraints constraints = repository.getConsumersWorkloadConstraints();

        // then
        Map<TopicName, Constraints> topicConstraints = constraints.getTopicConstraints();
        assertThat(topicConstraints.get(topic).getConsumersNumber()).isEqualTo(1);

        Map<SubscriptionName, Constraints> subscriptionConstraints = constraints.getSubscriptionConstraints();
        assertThat(subscriptionConstraints.get(subscription).getConsumersNumber()).isEqualTo(3);

        // and
        assertThat(listAppender.list).isEmpty();
    }

    @Test
    public void shouldLogWarnMessageIfDataFromZNodeCannotBeReadTest() throws Exception {
        // given
        setupPath("/hermes/consumers-workload-constraints/group.topic", "random data");

        // when
        ConsumersWorkloadConstraints constraints = repository.getConsumersWorkloadConstraints();

        // then
        assertThat(constraints.getTopicConstraints()).isEqualTo(emptyMap());

        // and
        assertThat(listAppender.list.get(0).getFormattedMessage())
                .isEqualTo("Error while reading data from node /hermes/consumers-workload-constraints/group.topic");
        assertThat(listAppender.list.get(0).getThrowableProxy().getClassName())
                .isEqualTo("com.fasterxml.jackson.databind.exc.MismatchedInputException");
        assertThat(listAppender.list.get(0).getLevel())
                .isEqualTo(Level.WARN);
    }
}
