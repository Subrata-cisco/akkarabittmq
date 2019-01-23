package com.subrata.akkaAmqpTest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import akka.Done;
import akka.NotUsed;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.amqp.AmqpConnectionProvider;
import akka.stream.alpakka.amqp.AmqpSinkSettings;
import akka.stream.alpakka.amqp.IncomingMessage;
import akka.stream.alpakka.amqp.NamedQueueSourceSettings;
import akka.stream.alpakka.amqp.QueueDeclaration;
import akka.stream.alpakka.amqp.javadsl.AmqpSink;
import akka.stream.alpakka.amqp.javadsl.AmqpSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

@RestController("/msg")
public class MessageController {
	
	
	@Autowired
	private AmqpConnectionProvider connectionProvider;
	
	@Autowired
	private QueueDeclaration queueDeclaration;
	
	@Autowired
	@Qualifier("materializer")
	private ActorMaterializer materializer;
	
	
	@RequestMapping(method=RequestMethod.POST)
	public void testMessage() throws IOException {
		System.out.println("********************* Got the call ...");
		
		  // #create-sink - producer
	    final Sink<ByteString, CompletionStage<Done>> amqpSink =
	        AmqpSink.createSimple(
	            AmqpSinkSettings.create(connectionProvider)
	                .withRoutingKey(AkkaConstants.queueName)
	                .withDeclaration(queueDeclaration));
	  
	    
	    // #run-sink
	    //final List<String> input = Arrays.asList("one", "two", "three", "four", "five");
	    //Source.from(input).map(ByteString::fromString).runWith(amqpSink, materializer);
	    
	    String filePath = "D:\\subrata\\code\\akkaAmqpTest-master\\akkaAmqpTest-master\\logs2\\dummy.txt";
	    Path path = Paths.get(filePath);
	    
	    // List containing 78198 individual message
	    List<String> contents = Files.readAllLines(path);
	    System.out.println("********** file reading done ....");
	    int times = 5;
	    
	    // Send 78198*times message to Queue [From console i can see 400000 number of messages being sent]
	    for(int i=0;i<times;i++) {
	    	Source.from(contents).map(ByteString::fromString).runWith(amqpSink, materializer);
	    }
	    System.out.println("************* sending to queue is done");

	    

	    // #create-source - consumer
	    final Integer bufferSize = 1;
	    final Source<IncomingMessage, NotUsed> amqpSource =
	        AmqpSource.atMostOnceSource(
	            NamedQueueSourceSettings.create(connectionProvider, AkkaConstants.queueName)
	                .withDeclaration(queueDeclaration),
	            bufferSize);
	    System.out.println("************* reading from queue is prepared...");
	   
	    // #run-source
	    final CompletionStage<List<IncomingMessage>> result =
	        amqpSource.take(times).runWith(Sink.seq(), materializer);
	    System.out.println("************* Strating reading from queue....");
	   

	    List<String> collect = new ArrayList<String>();
		try {
			collect = result
			.toCompletableFuture()
			.get(300, TimeUnit.MINUTES)
			.stream()
			.map(line -> line.bytes().utf8String()
				
			).collect(Collectors.toList());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    for (String s:collect) {    	
	    	System.out.println(s);
	    }
	    
		System.out.println("*******        ALL Done     *******");
	}
}
