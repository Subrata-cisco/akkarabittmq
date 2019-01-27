package com.subrata.akkaAmqpTest;

import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.context.annotation.Configuration;

@Configuration
public class ProducerGate {

	private AtomicInteger val = new AtomicInteger(1);

	public boolean isOpen() {
		return val.get() == 1;
	}

	public void openGate() {
		val.set(1);
	}

	public void closeGate() {
		val.set(0);
	}

}
