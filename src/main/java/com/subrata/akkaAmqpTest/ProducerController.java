package com.subrata.akkaAmqpTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

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
import akka.stream.alpakka.amqp.AmqpConnectionProvider;
import akka.stream.alpakka.amqp.AmqpSinkSettings;
import akka.stream.alpakka.amqp.QueueDeclaration;
import akka.stream.alpakka.amqp.javadsl.AmqpSink;
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
	
	@PostMapping(path="/start/{t}") 
	public ResponseEntity<String> produceStartMessage(@PathVariable("t") Optional<Integer>  times) {
		System.out.println("*************** ProducerController.produceStartMessage()");
		int itNo = 1;
		if(times.isPresent()) {
			itNo = times.get();
		}
		
		final Sink<ByteString, CompletionStage<Done>> amqpSink =
		        AmqpSink.createSimple(
		            AmqpSinkSettings.create(connectionProvider)
		                .withRoutingKey(AkkaConstants.queueName)
		                .withDeclaration(queueDeclaration));
		
		String message = "Hi this message is hardcoded in code... But can be passed as parameter as well....";
	    List<String> contents = new ArrayList<>();
	    contents.add(message);
	    for(int i=0;i<itNo;i++) {
	    	try {
	    		/*CompletionStage<ByteString> completion = */ 
	    		  Source.from(contents)
	    				.map(ByteString::fromString)
	    				.runWith(amqpSink, materializer);
	    				/*.handle((s, ex) -> { 
	    					if(s != null) {
	    					  System.out.println("********** s1 : "+s.getClass().getName()+" ex :"+ex);
	    					}
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
	    		
	    	} catch(Exception ex) {
	    		System.out.println("*********** Exception .... ProducerController.produceStartMessage()");
	    	}
	    }
	    System.out.println("****************** ProducerController.produceStartMessage() Total message sent :"+itNo);
	    return new ResponseEntity<>("Total message sent :"+itNo, HttpStatus.OK);
	}

}
