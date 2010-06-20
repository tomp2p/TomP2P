/*
 * Copyright 2009 Thomas Bocek
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package net.tomp2p.utils;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter
{
	@Override
	public String format(LogRecord record)
	{
		StringBuilder sb = new StringBuilder(String.format(
				"%3$s:[%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS:%1$tL] %2$s: %4$s\n",
				record.getMillis(), record.getSourceClassName(), record.getLevel(), record
						.getMessage()));
		if (record.getThrown() != null)
			sb.append(printThrown(record.getThrown())).append("\n");
		return sb.toString();
	}

	private StringBuilder printThrown(Throwable thrown)
	{
		StringBuilder sb = new StringBuilder(thrown.getClass().getName());
		sb.append(" - ").append(thrown.getMessage());
		sb.append("\n");
		for (StackTraceElement trace : thrown.getStackTrace())
			sb.append("\tat ").append(trace).append("\n");
		Throwable cause = thrown.getCause();
		if (cause != null)
			sb.append(printThrown(cause));
		return sb;
	}
}
