package nl.siepkes.hivemq.clienttest;

import com.hivemq.client.mqtt.lifecycle.MqttClientAutoReconnect;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMqMqttClient {

	private static final Logger log = LoggerFactory.getLogger(HiveMqMqttClient.class);

	private final String host = "127.0.0.1";
	private final int port = 1883;

	private volatile Mqtt5AsyncClient mqttClient;

	private final ExecutorService callbackExecutor;

	public HiveMqMqttClient() {
		final AtomicLong mqttCallbackThreadId = new AtomicLong();
		callbackExecutor = Executors.newFixedThreadPool(2, r -> {
			Thread thread = new Thread(r);
			thread.setName(String.format("MqttCallbackExecutor-%d", mqttCallbackThreadId.incrementAndGet()));
			thread.setDaemon(false);
			return thread;
		});
	}

	public CompletionStage<Void> connect() {
		Mqtt5ClientBuilder clientBuilder = Mqtt5Client.builder()
				.automaticReconnect(MqttClientAutoReconnect.builder()
						.initialDelay(500, TimeUnit.MILLISECONDS)
						.maxDelay(10, TimeUnit.SECONDS)
						.build())
				.identifier(UUID.randomUUID().toString())
				.serverHost(host)
				.serverPort(1883)
				.addDisconnectedListener(context -> {
					if(log.isTraceEnabled()) {
						log.trace("MQTT client disconnected.", context.getCause());
					} else {
						log.debug("MQTT client disconnected with error: '{}'.", context.getCause().getMessage());
					}
				})
				.addConnectedListener(context -> log.info ("MQTT client (re-)connected to broker '{}:{}'.", host, port));

		mqttClient = clientBuilder.buildAsync();
		return mqttClient.connect().thenApply(mqtt5ConnAck -> null);
	}

	public CompletionStage<Mqtt5SubAck> subscribeRandom() {
		return mqttClient.subscribeWith()
				.topicFilter("topic/without/"+UUID.randomUUID().toString())
				.callback(mqttPublish -> log.info("Message received."))
				.executor(callbackExecutor)
				.send()
				.toCompletableFuture();
	}

}
