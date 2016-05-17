package com.ppk.kafka.stream.rx;

import org.springframework.stereotype.Component;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.Subscription;

@Component
public class AsynchObservable {

	@SuppressWarnings("rawtypes")
	public Subscription sendAsynCall(final Runnable runnable, Scheduler scheduler) {
		Observable observable = Observable.create(new Observable.OnSubscribe<Void>() {
			@Override
			public void call(Subscriber<? super Void> observer) {
				try {
					if (!observer.isUnsubscribed()) {
						runnable.run();
						observer.onCompleted();
					}
				} catch (Exception e) {
					observer.onCompleted();
				}
			}
		});
		return observable.subscribeOn(scheduler).subscribe();
	}
}
