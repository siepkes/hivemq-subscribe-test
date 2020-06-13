package nl.siepkes.hivemq.clienttest;

import com.hivemq.embedded.EmbeddedHiveMQ;
import com.hivemq.embedded.EmbeddedHiveMQBuilder;
import java.nio.file.Path;
import java.util.concurrent.CompletionStage;


public class EmbeddedBroker {

	private final Path brokerRoot;

	private volatile EmbeddedHiveMQ hiveMQ;

	public EmbeddedBroker(Path brokerRoot) {
		this.brokerRoot = brokerRoot;
	}

	public CompletionStage<Void> start() {
		final EmbeddedHiveMQBuilder embeddedHiveMQBuilder = EmbeddedHiveMQBuilder.builder()
				.withConfigurationFolder(brokerRoot.resolve("config"))
				//.withDataFolder(brokerRoot.resolve("data"))
				.withExtensionsFolder(brokerRoot.resolve("extensions"));

		hiveMQ = embeddedHiveMQBuilder.build();

		return hiveMQ.start();
	}

	public CompletionStage<Void> stop() {
			return hiveMQ.stop();
	}
}
