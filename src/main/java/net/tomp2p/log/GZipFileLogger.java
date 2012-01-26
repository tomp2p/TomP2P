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
package net.tomp2p.log;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.zip.GZIPOutputStream;

import net.tomp2p.utils.Timing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class GZipFileLogger implements PeerLogger
{
	final private static Logger logger = LoggerFactory.getLogger(GZipFileLogger.class);
	final private PrintWriter pw;

	public GZipFileLogger(final File file) throws IOException
	{
		if (!file.getName().endsWith(".gz"))
			logger.warn("Filename does not end with .gz. This is not recommended");
		pw = new PrintWriter(new GZIPOutputStream(new FileOutputStream(file)));
	}

	@Override
	public void sendLog(final String name, final String value)
	{
		StringBuilder sb = new StringBuilder("[").append(name).append("]=[").append(value).append(
				"],time[").append(Timing.currentTimeMillis()).append("]");
		pw.println(sb.toString());
	}

	@Override
	public void shutdown()
	{
		pw.close();
	}
}
