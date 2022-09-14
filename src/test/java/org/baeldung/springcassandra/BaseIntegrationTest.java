package org.baeldung.springcassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

@RunWith(SpringRunner.class)
@SpringBootTest
@WebAppConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@TestPropertySource(locations = "classpath:application.properties")
@ContextConfiguration(initializers = {BaseIntegrationTest.Initializer.class})
public class BaseIntegrationTest {

    public static TestRule tr = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            TestcontainersConfiguration.getInstance().updateUserConfig("testcontainers.reuse.enable", "true");
            base.evaluate();
        }
    };

    public static CassandraContainer cassandra = (CassandraContainer) new CassandraContainer("cassandra:4.0.5")
            .withExposedPorts(9042)
            .withEnv("HEAP_NEWSIZE", "128M")
            .withEnv("MAX_HEAP_SIZE", "1024M")
            .withEnv("JVM_OPTS", "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.initial_token=0")
            .withEnv("CASSANDRA_SNITCH", "GossipingPropertyFileSnitch")
            .withReuse(true);

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(tr)
            .around(cassandra);


    static class Initializer
            implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            Cluster cluster = cassandra.getCluster();

            System.setProperty("spring.data.cassandra.keyspace-name", "test");
            System.setProperty("spring.data.cassandra.contact-points", cassandra.getContainerIpAddress());
            System.setProperty("spring.data.cassandra.port", String.valueOf(cassandra.getMappedPort(9042)));

            Session session = cluster.connect();
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + "test" + " WITH replication = \n" +
                    "{'class':'SimpleStrategy','replication_factor':'1'};");
            session.execute(
                    "CREATE TABLE IF NOT EXISTS " + "test" + "." + "proof_of_possession" + "\n" +
                            "(\n" +
                            "    id                    bigint primary key,\n" +
                            "    created               timestamp,\n" +
                            "    created_by            text,\n" +
                            "    customer_order_number text,\n" +
                            "    payload               text,\n" +
                            "    sent_to_com           boolean\n" +
                            ");");

            Host host = cassandra.getCluster().getMetadata().getAllHosts().stream().findFirst().orElseThrow();

            TestPropertyValues.of(
                    "cassandra_data_center=" + host.getDatacenter(),
                    "cassandra_contact_points=" + cassandra.getHost() + ":" + cassandra.getMappedPort(9042),
                    "cassandraKeyspace=" + "test"
            ).applyTo(configurableApplicationContext.getEnvironment());
        }
    }
}
