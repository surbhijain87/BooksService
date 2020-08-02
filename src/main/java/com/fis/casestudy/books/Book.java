package com.fis.casestudy.books;

import java.util.Objects;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Book {

	@Id
	private String bookId;
	private String name;
	private String author;
	private int available;
	private int total;

	public Book() {
	}

	public Book(String bookId, String name, String author, int available, int total) {
		this.bookId = bookId;
		this.name = name;
		this.author = author;
		this.available = available;
		this.total = total;
	}

	public void setBookId(String bookId) {
		this.bookId = bookId;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public void setAvailable(int available) {
		this.available = available;
	}

	public void setTotal(int total) {
		this.total = total;
	}

	public String getBookId() {
		return bookId;
	}

	public String getName() {
		return name;
	}

	public String getAuthor() {
		return author;
	}

	public int getAvailable() {
		return available;
	}

	public int getTotal() {
		return total;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o)
			return true;
		if (!(o instanceof Book))
			return false;
		Book book = (Book) o;
		return Objects.equals(this.bookId, book.bookId) && Objects.equals(this.name, book.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.bookId, this.name);
	}

	@Override
	public String toString() {
		return "Book [bookId=" + bookId + ", name=" + name + ", author=" + author + ", available=" + available
				+ ", total=" + total + "]";
	}
}
