package com.sri.kafka;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaSimpleController {

	private Logger log = LoggerFactory.getLogger(KafkaSimpleController.class);

//	@Autowired
	private KafkaTemplate<String, User> kafkaTemplate;

	@Autowired
	public KafkaSimpleController(KafkaTemplate<String, User> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

//	private ConcurrentKafkaListenerContainerFactory<String, User> kafkaConsumerFactory;
//
//	@Autowired
//	public KafkaSimpleController(ConcurrentKafkaListenerContainerFactory<String, User> kafkaConsumerFactory) {
//		this.kafkaConsumerFactory = kafkaConsumerFactory;
//	}

	@GetMapping("/user/test")
	public User getUser() {
		return new User("firstname", "lastname");
	}

	@PostMapping("/user/add")
	public void ProduceToKafka(@RequestBody User user) {

		log.info("received User" + user);

		ProducerRecord<String, User> message = new ProducerRecord<String, User>("twitter_status", user);
		ListenableFuture<SendResult<String, User>> send = kafkaTemplate.send(message);

		try {
			log.info("record metadata" + send.get().getRecordMetadata());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@KafkaListener(topics = "twitter_status")
	public void ConsumeToTopic(User user) {

		log.info("received Message in Group user_consumer" + user.toString());
	}
}
