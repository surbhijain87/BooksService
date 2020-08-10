package com.fis.casestudy.books;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.transaction.IllegalTransactionStateException;
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

		return repository.findById(bookId).map(book -> {
			int availableCount = book.getAvailable();

			if (availableCount + incrementCount <= book.getTotal()) {
				book.setAvailable(availableCount + incrementCount);
				checkForNotifications(bookId);
				return repository.save(book);
			}

			String exceptionMessage = String.format("Available count %s cannot exceed Total Count %s for book %s",
					book.getAvailable(), book.getTotal(), book.getName());
			throw new IllegalTransactionStateException(exceptionMessage);
		}).orElseThrow(() -> new BookNotFoundException(bookId));

	}

	void checkForNotifications(String bookId) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("member.id", "ID1234");
		props.put("group.id", "Group1");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props)) {
			consumer.subscribe(Arrays.asList("NotificationTopic"));

			ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));

			for (ConsumerRecord<String, String> message : messages) {
				System.out.printf("Offset = %d, Value = %s\n", message.offset(), message.key(), message.value());
				// Send email for each
			}
		}

	}

}
