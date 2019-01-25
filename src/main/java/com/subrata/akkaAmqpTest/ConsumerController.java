package com.subrata.akkaAmqpTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.batch.BatchProperties.Job;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import akka.NotUsed;
import akka.stream.ActorMaterializer;
import akka.stream.OverflowStrategy;
import akka.stream.alpakka.amqp.AmqpConnectionProvider;
import akka.stream.alpakka.amqp.IncomingMessage;
import akka.stream.alpakka.amqp.NamedQueueSourceSettings;
import akka.stream.alpakka.amqp.QueueDeclaration;
import akka.stream.alpakka.amqp.javadsl.AmqpSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;

@RestController
@RequestMapping("consume")
public class ConsumerController {
	
	@Autowired
	private AmqpConnectionProvider connectionProvider;
	
	@Autowired
	private QueueDeclaration queueDeclaration;
	
	@Autowired
	@Qualifier("materializer")
	private ActorMaterializer materializer;
	
	@PostMapping("/start/{c}")
	public ResponseEntity<String> consumeStartMessage(@PathVariable("c") Optional<Integer>  count) {
		int totalC = 1;
		if(count.isPresent()) {
			totalC = count.get();
		}
		System.out.println("*************** ConsumerController.consumeStartMessage() consume requested :"+totalC);
		
		final Integer bufferSize = 1;
	    final Source<IncomingMessage, NotUsed> amqpSource =
	        AmqpSource.atMostOnceSource(
	            NamedQueueSourceSettings.create(connectionProvider, AkkaConstants.queueName)
	                .withDeclaration(queueDeclaration),
	            bufferSize);
	    System.out.println("************* Reading from queue is prepared... is empty ? : "+amqpSource.empty());
	    
	   
	    // #run-source
	    final CompletionStage<List<IncomingMessage>> result =
	        amqpSource.take(totalC).runWith(Sink.seq(), materializer);
	    System.out.println("************* Strating reading from queue....");
	   

	    List<String> collect = new ArrayList<String>();
		try {
			collect = result
			.toCompletableFuture()
			.get(300, TimeUnit.MINUTES)
			.stream()
			.map(line -> line.bytes().utf8String()
				
			).collect(Collectors.toList());
		} catch (InterruptedException|ExecutionException|TimeoutException e) {
			e.printStackTrace();
		}
		
		int totalMesgReceived = 0;
	    for (String s:collect) {    	
	    	System.out.println(s);
	    	totalMesgReceived++;
	    }
	    
	    try {
			Thread.sleep(5000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    return new ResponseEntity<>("Total message received :"+totalMesgReceived, HttpStatus.OK);
	}
	
	@PostMapping("/stop")
	public void stopConsumingMessage() {
		
		 final Source<Job, NotUsed> inboundJobsConnector = Source.empty();
		    //#explicit-buffers-backpressure
		    // Getting a stream of jobs from an imaginary external system as a Source
		    final Source<Job, NotUsed> jobs = inboundJobsConnector;
		    jobs.buffer(1000, OverflowStrategy.backpressure());
		    //#explicit-buffers-backpressure

		    //#explicit-buffers-droptail
		    jobs.buffer(1000, OverflowStrategy.dropTail());
		    //#explicit-buffers-droptail

		    //#explicit-buffers-dropnew
		    jobs.buffer(1000, OverflowStrategy.dropNew());
		    //#explicit-buffers-dropnew

		    //#explicit-buffers-drophead
		    jobs.buffer(1000, OverflowStrategy.dropHead());
		    //#explicit-buffers-drophead

		    //#explicit-buffers-dropbuffer
		    jobs.buffer(1000, OverflowStrategy.dropBuffer());
		    //#explicit-buffers-dropbuffer

		    //#explicit-buffers-fail
		    jobs.buffer(1000, OverflowStrategy.fail());
		    //#explicit-buffers-fail
		
	}

}
