package com.teamsun.porters.move.exception;

import java.io.PrintStream;
import java.io.PrintWriter;

public class BaseException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private Throwable throwable;

	public BaseException() {
		super();
	}

	public BaseException(String msg) {
		super(msg);

	}

	public BaseException(String msg, Throwable throwable) {
		super(msg, throwable);
	}

	public Throwable getException() {
		throwable = super.getCause();

		return throwable;
	}

	public void printStackTrace() {
		super.printStackTrace();
	}

	public void printStackTrace(PrintStream printStream) {
		super.printStackTrace(printStream);
	}

	public void printStackTrace(PrintWriter printWriter) {
		super.printStackTrace(printWriter);

	}

}
