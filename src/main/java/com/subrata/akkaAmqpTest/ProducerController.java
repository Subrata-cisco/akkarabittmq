package com.subrata.akkaAmqpTest;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import akka.Done;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.OverflowStrategy;
import akka.stream.alpakka.amqp.AmqpConnectionProvider;
import akka.stream.alpakka.amqp.AmqpSinkSettings;
import akka.stream.alpakka.amqp.QueueDeclaration;
import akka.stream.alpakka.amqp.javadsl.AmqpSink;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

@RestController
@RequestMapping("produce")
public class ProducerController {
	
	@Autowired
	private AmqpConnectionProvider connectionProvider;
	
	@Autowired
	private QueueDeclaration queueDeclaration;
	
	@Autowired
	@Qualifier("materializer")
	private ActorMaterializer materializer;
	
	@Autowired
	private ProducerGate pg;
	
	@PostMapping(path="/start") 
	public ResponseEntity<String> produceStartMessage() {
		System.out.println("*************** ProducerController.produceStartMessage()");
		final Sink<ByteString, CompletionStage<Done>> amqpSink =
		        AmqpSink.createSimple(
		            AmqpSinkSettings.create(connectionProvider)
		            //.
		                .withRoutingKey(AkkaConstants.queueName)
		                .withDeclaration(queueDeclaration));
		
		String message = "Hi this message is hardcoded in code...";
	    List<String> contents = new ArrayList<>();
	    contents.add(message);
	    
	    
	    	
	    int mc = 0;
	    
	while(pg.isOpen()) {
	    	try {
	    		CompletionStage<Done> result =  Source.from(contents)
	    		        //.buffer(10, OverflowStrategy.backpressure())
	    		        .conflate((s1,s2) -> s1+"$$$$$"+s2 )
	    				.map(ByteString::fromString)
	    				.runWith(amqpSink, materializer);
	    		mc++;
	    		System.out.println("***** Message no :"+mc+" initiated.....");
	    		
	    		Thread.sleep(10);
	    		
	    		/*try {
	    		      result.toCompletableFuture().get();
	    		    } catch (ExecutionException e) {
	    		    	System.out.println("ERRORROOR  ProducerController.produceStartMessage()"+e.getMessage());
	    		      //throw e.getCause();
	    		    }*/
	    		
	    				/*.handle((s, ex) -> { 
	    					if(ex instanceof IOException) {
	    						System.out.println("ProducerController.produceStartMessage()"+ex.getMessage());
	    					}
	    					System.out.println("********** s1 : "+s+" ex :"+ex);
	    					return null;
	    				 })
						.exceptionally(th -> {
							System.out.println("********** s2 : "+th);
							return null;
						})
						
						.whenComplete((s, ex) -> {
							System.out.println("********** s3 : "+s+" ex :"+ex);
							if(s != null) {
								System.out.println("************** Message delivery failed....");
							}
						});*/
	    		  //System.out.println("ProducerController.produceStartMessage()");
	    		
	    	} catch(Exception ex) {
	    		System.out.println("*********** Exception .... ProducerController.produceStartMessage()");
	    	}
	    }
	    System.out.println("****************** ProducerController.produceStartMessage() Total message sent :"+mc);
	    return new ResponseEntity<>("Total message sent requested :"+mc, HttpStatus.OK);
	}
	
	public Sink<String, CompletionStage<IOResult>> lineSink(String filename) {
		  return Flow.of(String.class)
		    .map(s -> ByteString.fromString(s.toString() + "\n"))
		    .toMat(FileIO.toPath(Paths.get(filename)), Keep.right());
		}
	
	@PostMapping(path="/gate/{t}") 
	public ResponseEntity<String> changeGate(@PathVariable("t") Integer  status) {
		if(status == 1) {
			pg.openGate();
		}else {
			pg.closeGate();
		}
		 
		return new ResponseEntity<>("GateStatus changed to :"+pg.isOpen(), HttpStatus.OK);
		
	}

}
