package com.ppk.kafka.stream;

import java.util.Map;
import java.util.concurrent.Future;

public interface Producer {
	Boolean send(Object body);

	Future<Boolean> sendAsync(Object body);

	Boolean send(Object body, Map<String, String> headers);

	Future<Boolean> sendAsync(Object body, Map<String, String> headers);

	Boolean send(Object body, String authrizationHeader, Map<String, String> headers);

	Future<Boolean> sendAsync(Object body, String authrizationHeader, Map<String, String> headers);

	void sendAsyncObservable(Object body, String authrizationHeader, Map<String, String> headers);
}
