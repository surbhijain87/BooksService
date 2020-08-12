package com.fis.casestudy.books;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BooksController {

	private final BookRepository repository;

	public BooksController(BookRepository repository) {
		this.repository = repository;
	}

	@RequestMapping("/books")
	public List<Book> index() {
		return repository.findAll();
	}

	@RequestMapping("/books/{id}")
	public Book getBooksById(@PathVariable("id") String id) {
		System.out.println("Getting request for " + id);
		return repository.findById(id).orElseThrow(() -> new BookNotFoundException(id));
	}

	@PostMapping("/books/updateAvailability/{bookId}/{incrementCount}")
	public Book update(@PathVariable("bookId") String bookId, @PathVariable("incrementCount") int incrementCount) {
		System.out.println("Update request for " + bookId);
		return repository.findById(bookId).map(book -> {
			int availableCount = book.getAvailable();

			if (availableCount + incrementCount <= book.getTotal() && availableCount + incrementCount >= 0) {
				book.setAvailable(availableCount + incrementCount);

				if (availableCount == 0 && incrementCount > 0) {
					checkForNotifications(bookId);
				}
				return repository.save(book);
			}

			String exceptionMessage = String.format("Available count %s cannot exceed Total Count %s for book %s",
					book.getAvailable(), book.getTotal(), book.getName());
			throw new IllegalTransactionStateException(exceptionMessage);
		}).orElseThrow(() -> new BookNotFoundException(bookId));

	}

	void checkForNotifications(String bookId) {
		System.out.println("check for notifications ");

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("member.id", "ID1234");
		props.put("group.id", "Group1");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("max.poll.records", "10");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {
			String topic = "NotificationTopic" + bookId;
			consumer.subscribe(Arrays.asList(topic));

			ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));

			for (ConsumerRecord<String, String> message : messages) {
				System.out.printf("Send book availability notification for Book = %s to User = %s\n", bookId,
						message.value());
			}
		}

	}

}
