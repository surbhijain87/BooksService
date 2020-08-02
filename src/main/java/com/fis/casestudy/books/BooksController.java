package com.fis.casestudy.books;

import java.util.List;

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
		return repository.findById(id).orElseThrow(() -> new BookNotFoundException(id));
	}

	@PostMapping("/books/updateAvailability/{bookId}/{incrementCount}")
	public Book getBooksById(@PathVariable("bookId") String bookId,
			@PathVariable("incrementCount") int incrementCount) {

		return repository.findById(bookId).map(book -> {
			int availableCount = book.getAvailable();

			if (availableCount + incrementCount <= book.getTotal()) {
				book.setAvailable(availableCount + incrementCount);
				return repository.save(book);
			}

			String exceptionMessage = String.format(
					"Available count %s cannot exceed Total Count %s for book %s" , book.getAvailable(),
					book.getTotal(), book.getName());
			throw new IllegalTransactionStateException(exceptionMessage);
		}).orElseThrow(() -> new BookNotFoundException(bookId));

	}

}
