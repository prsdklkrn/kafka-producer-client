package com.ppk.kafka.stream;

import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import com.ppk.kafka.stream.model.Message;
import com.ppk.kafka.stream.rx.AsynchObservable;

import rx.schedulers.Schedulers;

@Service
@EnableBinding(Source.class)
public class ProducerImpl implements Producer {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProducerImpl.class);

	@Autowired
	private AsynchObservable asynchObservable;

	@Autowired
	private Source outboundChannel;

	@Override
	public Boolean send(Object body) {
		return outboundChannel.output().send(MessageBuilder.withPayload(body).build());
	}

	@Async
	@Override
	public Future<Boolean> sendAsync(Object body) {
		boolean result = false;
		try {
			result = outboundChannel.output().send(MessageBuilder.withPayload(body).build());
		} catch (Exception e) {
			LOGGER.error("Failed to send message to Kafka. ", e);
		}
		return new AsyncResult<>(result);
	}

	@Override
	public Boolean send(Object body, Map<String, String> headers) {
		Message message = new Message();
		message.setHeaders(headers);
		message.setContent(body);
		return outboundChannel.output().send(MessageBuilder.withPayload(message).build());
	}

	@Async
	@Override
	public Future<Boolean> sendAsync(Object body, Map<String, String> headers) {
		Message message = new Message();
		message.setHeaders(headers);
		message.setContent(body);
		boolean result = false;
		try {
			result = outboundChannel.output().send(MessageBuilder.withPayload(message).build());
		} catch (Exception e) {
			LOGGER.error("Failed to send message to Kafka. ", e);
		}
		return new AsyncResult<>(result);
	}

	@Override
	public Boolean send(Object body, String authrizationHeader, Map<String, String> headers) {
		Message message = new Message();
		message.setAuthorizationHeader(authrizationHeader);
		message.setHeaders(headers);
		message.setContent(body);
		return outboundChannel.output().send(MessageBuilder.withPayload(message).build());
	}

	@Async
	@Override
	public Future<Boolean> sendAsync(Object body, String authrizationHeader, Map<String, String> headers) {
		Message message = new Message();
		message.setAuthorizationHeader(authrizationHeader);
		message.setHeaders(headers);
		message.setContent(body);
		boolean result = false;
		try {
			result = outboundChannel.output().send(MessageBuilder.withPayload(message).build());
		} catch (Exception e) {
			LOGGER.error("Failed to send message to Kafka. ", e);
		}
		return new AsyncResult<>(result);
	}

	@Override
	public void sendAsyncObservable(Object body, String authrizationHeader, Map<String, String> headers) {
		Message message = new Message();
		message.setAuthorizationHeader(authrizationHeader);
		message.setHeaders(headers);
		message.setContent(body);
		sendMessage(message);
	}

	private void sendMessage(Object body) {
		asynchObservable.sendAsynCall(() -> {
			outboundChannel.output().send(MessageBuilder.withPayload(body).build());
		}, Schedulers.io());
	}

}
