package com.fis.casestudy.books;

public class BookNotFoundException extends RuntimeException {

	public  BookNotFoundException(String id) {
	    super("Could not find employee " + id);
	  } 
}
