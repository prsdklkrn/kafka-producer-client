package com.ppk.kafka.stream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.springframework.cloud.stream.test.matcher.MessageQueueMatcher.receivesPayloadThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.cloud.stream.annotation.Bindings;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = TestMain.class)
public class MessengerImplTest {

	@Autowired
	@Bindings(ProducerImpl.class)
	private Source channels;

	@Autowired
	private MessageCollector messageCollector;

	@Autowired
	private Producer messenger;

	@Test
	public void testSendMessage() throws Exception {
		boolean res = messenger.send("ProducerTest");
		assertThat(messageCollector.forChannel(channels.output()), receivesPayloadThat(is("ProducerTest")));
		assertTrue(res);
	}

	@Test
	public void testSendAsyncMessage() throws Exception {
		Future<Boolean> messangerTest = messenger.sendAsync("ProducerTest");
		Boolean result = messangerTest.get();
		assertTrue(result);
		assertThat(messageCollector.forChannel(channels.output()), receivesPayloadThat(is("ProducerTest")));
	}

	@Test
	public void testSendWithHeader() throws Exception {
		Map<String, String> headers = new HashMap<>();
		headers.put("HeaderKey", "HeaderValue");
		boolean res = messenger.send("ProducerTest", headers);
		Message<?> received = messageCollector.forChannel(channels.output()).poll();
		assertTrue(received.getPayload().toString().contains("HeaderKey"));
		assertTrue(received.getPayload().toString().contains("HeaderValue"));
		assertTrue(received.getPayload().toString().contains("ProducerTest"));
		assertTrue(res);
	}

	@Test
	public void testSendAsyncWithHeader() throws Exception {
		Map<String, String> headers = new HashMap<>();
		headers.put("HeaderKey", "HeaderValue");
		Future<Boolean> messangerTest = messenger.sendAsync("ProducerTest", headers);
		Boolean result = messangerTest.get();
		Message<?> received = messageCollector.forChannel(channels.output()).poll();
		assertTrue(received.getPayload().toString().contains("HeaderKey"));
		assertTrue(received.getPayload().toString().contains("HeaderValue"));
		assertTrue(received.getPayload().toString().contains("ProducerTest"));
		assertTrue(result);
	}

	@Test
	public void testSendWithHeaderAndAuth() throws Exception {
		String authHeader = "MyAuthorization";
		Map<String, String> headers = new HashMap<>();
		headers.put("HeaderKey", "HeaderValue");
		boolean res = messenger.send("ProducerTest", authHeader, headers);
		Message<?> received = messageCollector.forChannel(channels.output()).poll();
		assertTrue(received.getPayload().toString().contains("HeaderKey"));
		assertTrue(received.getPayload().toString().contains("HeaderValue"));
		assertTrue(received.getPayload().toString().contains("ProducerTest"));
		assertTrue(received.getPayload().toString().contains(authHeader));
		assertTrue(res);
	}

	@Test
	public void testSendAsyncWithHeaderAndAuth() throws Exception {
		String authHeader = "MyAuthorization";
		Map<String, String> headers = new HashMap<>();
		headers.put("HeaderKey", "HeaderValue");
		Future<Boolean> messangerTest = messenger.sendAsync("ProducerTest", authHeader, headers);
		Boolean result = messangerTest.get();
		Message<?> received = messageCollector.forChannel(channels.output()).poll();
		assertTrue(received.getPayload().toString().contains("HeaderKey"));
		assertTrue(received.getPayload().toString().contains("HeaderValue"));
		assertTrue(received.getPayload().toString().contains("ProducerTest"));
		assertTrue(received.getPayload().toString().contains(authHeader));
		assertTrue(result);
	}

	@Test
	public void testSendAsyncObservable() throws Exception {
		String authHeader = "MyAuthorization";
		Map<String, String> headers = new HashMap<>();
		headers.put("HeaderKey", "HeaderValue");
		messenger.sendAsyncObservable("ProducerTest", authHeader, headers);
		Thread.sleep(300);
		Message<?> received = messageCollector.forChannel(channels.output()).poll();
		assertTrue(received.getPayload().toString().contains("HeaderKey"));
		assertTrue(received.getPayload().toString().contains("HeaderValue"));
		assertTrue(received.getPayload().toString().contains("ProducerTest"));
		assertTrue(received.getPayload().toString().contains(authHeader));
	}
}