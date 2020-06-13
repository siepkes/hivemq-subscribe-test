package nl.siepkes.hivemq.clienttest;

import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SubscribeTest {

	private static final Logger log = LoggerFactory.getLogger(SubscribeTest.class);

	public static void main(String[] args) throws Exception {

		EmbeddedBroker broker = new EmbeddedBroker(Path.of("/home/siepkes/git/sp-projects/hivemq-client-test/src/main/broker"));
		broker.start()
				.thenApply(ack -> {
					log.info("Broker started.");
					return null;
				}).toCompletableFuture().get();

		final HiveMqMqttClient mqMqttClient = new HiveMqMqttClient();
		mqMqttClient.connect()
				.thenApply(ack -> {
					log.info("Client connected");
					return null;
				})
				.thenCompose(oVoid -> mqMqttClient.subscribeRandom())
				.thenApply(ack -> {
					log.info("Subscribe 1");
					return null;
				})
				.thenCompose(oVoid -> mqMqttClient.subscribeRandom())
				.thenApply(ack -> {
					log.info("Subscribe 2");
					return null;
				})
				.thenCompose(oVoid -> mqMqttClient.subscribeRandom())
				.thenApply(ack -> {
					log.info("Subscribe 3");
					return null;
				})
				.thenCompose(oVoid -> mqMqttClient.subscribeRandom())
				.thenApply(ack -> {
					log.info("Subscribe 4");
					return null;
				}).toCompletableFuture().get();
	}
}
