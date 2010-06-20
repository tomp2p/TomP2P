/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2010 Oracle.  All rights reserved.
 *
 */

package com.sleepycat.je.util;

import java.io.IOException;

/**
 * Thrown during log verification if a checksum cannot be verified or a log
 * entry is determined to be invalid by examining its contents.
 *
 * <p>This class extends {@code IOException} so that it can be thrown by the
 * {@code InputStream} methods of {@link LogVerificationInputStream}.</p>
 */
public class LogVerificationException extends IOException {

    public LogVerificationException(final String message) {
        super(message);
    }

    public LogVerificationException(final String message,
                                    final Throwable cause) {
        super(message);
        initCause(cause);
    }
}
