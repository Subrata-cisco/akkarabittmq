package com.subrata.akkaAmqpTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.TopologyRecoveryException;
import com.rabbitmq.client.UnblockedCallback;

import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.amqp.AmqpConnectionFactoryConnectionProvider;
import akka.stream.alpakka.amqp.AmqpConnectionProvider;
import akka.stream.alpakka.amqp.AmqpCredentials;
import akka.stream.alpakka.amqp.AmqpDetailsConnectionProvider;
import akka.stream.alpakka.amqp.QueueDeclaration;

@Configuration
public class AkkaConfiguration {
	
	@Autowired
	private ProducerGate pg;
	
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
		
		/*
		Map<String,Object> map = new HashMap<>();
		map.put("connection.blocked", "true");
		map.put("connection.unblocked", "true");
		
		map.put("authentication_failure_close", "true");
		map.put("basic.nack", "true");
		map.put("consumer_cancel_notify", "true");
		map.put("exchange_exchange_bindings", "true");
		map.put("publisher_confirms", "true");
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setClientProperties(map);
		AmqpConnectionProvider connectionProvider = AmqpConnectionFactoryConnectionProvider.create(factory)
				.withHostAndPort("localhost", 5672);
		Connection conn = null;
		try {
			conn = factory.newConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn = connectionProvider.get();
		System.out.println("AkkaConfiguration.connectionProvider() open :"+conn.isOpen());
		
		conn.addShutdownListener(new ShutdownListener() {
			@Override
			public void shutdownCompleted(ShutdownSignalException cause) {
				// TODO Auto-generated method stub
				System.out.println(
						"AkkaConfiguration.connectionProvider().new ShutdownListener() {...}.shutdownCompleted()");
			}
		});
		conn.addBlockedListener(new BlockedCallback() {
			@Override
			public void handle(String reason) throws IOException {
				// TODO Auto-generated method stub
				System.out.println("AkkaConfiguration.connectionProvider().new BlockedCallback() {...}.handle()");
			}
		}, new UnblockedCallback() {
			@Override
			public void handle() throws IOException {
				// TODO Auto-generated method stub
				System.out.println("AkkaConfiguration.connectionProvider().new UnblockedCallback() {...}.handle()");
			}
		});
		*/
		
		AmqpCredentials  amqpCredentials = new AmqpCredentials("guest", "guest");
		AmqpDetailsConnectionProvider connectionProvider = AmqpDetailsConnectionProvider.create("invalid", 5673)
				.withHostsAndPorts(Arrays.asList(Pair.create("localhost", 5672), Pair.create("localhost", 5674)))
				.withConnectionName("subratarconn")
				//.withConnectionTimeout(0)
				.withExceptionHandler(new ExceptionHandler() { 
					
					@Override
					public void handleBlockedListenerException(Connection connection, Throwable exception) {
						System.out.println(
								"******************** AkkaConfiguration.handleBlockedListenerException()");
						pg.closeGate();
					}
					
					@Override
					public void handleUnexpectedConnectionDriverException(Connection conn, Throwable exception) {
						System.out.println(
								"************************ AkkaConfiguration.connectionProvider()"
								+exception.getMessage()+" is open :"+conn.isOpen());
					}
					
					@Override
					public void handleTopologyRecoveryException(Connection conn, Channel ch, TopologyRecoveryException exception) {
						// TODO Auto-generated method stub
						System.out.println(
								"AkkaConfiguration.handleTopologyRecoveryException()");
					}
					
					@Override
					public void handleReturnListenerException(Channel channel, Throwable exception) {
						// TODO Auto-generated method stub
						System.out.println(
								"AkkaConfiguration.handleReturnListenerException()");
						
					}
					
					@Override
					public void handleConsumerException(Channel channel, Throwable exception, Consumer consumer, String consumerTag,
							String methodName) {
						// TODO Auto-generated method stub
						System.out.println(
								"AkkaConfiguration.handleConsumerException()");
						
					}
					
					@Override
					public void handleConnectionRecoveryException(Connection conn, Throwable exception) {
						// TODO Auto-generated method stub
						System.out.println(
								"AkkaConfiguration.handleConnectionRecoveryException()");
						
					}
					
					@Override
					public void handleConfirmListenerException(Channel channel, Throwable exception) {
						// TODO Auto-generated method stub
						System.out.println(
								"AkkaConfiguration.connectionProvider().new ExceptionHandler() {...}.handleConfirmListenerException()");
						
					}
					
					@Override
					public void handleChannelRecoveryException(Channel ch, Throwable exception) {
						// TODO Auto-generated method stub
						System.out.println(
								"AkkaConfiguration.connectionProvider().new ExceptionHandler() {...}.handleChannelRecoveryException()");
						
					}
				})
				.withCredentials(amqpCredentials);
		return connectionProvider;
	}

}
