package com.soumith.akkaAmqpTest;

import java.util.Arrays;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.amqp.AmqpConnectionProvider;
import akka.stream.alpakka.amqp.AmqpDetailsConnectionProvider;
import akka.stream.alpakka.amqp.QueueDeclaration;

@Configuration
public class AkkaConfiguration {
	
	
	@Bean
	public ActorMaterializer materializer() {
		ActorSystem actorSystem = ActorSystem.create("actor-system");
		
		ActorMaterializer materializer = ActorMaterializer.create(actorSystem);
		return materializer;
	}
	
	@Bean
	public QueueDeclaration queueDeclaration() {
		QueueDeclaration queueDeclaration = QueueDeclaration.create(AkkaConstants.queueName);
		return queueDeclaration;
		
	}
	
	@Bean
	public AmqpConnectionProvider connectionProvider() {
		   AmqpDetailsConnectionProvider connectionProvider =
			        AmqpDetailsConnectionProvider.create("invalid", 5673)
			            .withHostsAndPorts(
			                Arrays.asList(Pair.create("localhost", 5672), Pair.create("localhost", 5674)));
	
	return connectionProvider;
	}

}
