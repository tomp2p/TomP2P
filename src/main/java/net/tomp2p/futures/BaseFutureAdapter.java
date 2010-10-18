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
package net.tomp2p.futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseFutureAdapter<F extends BaseFuture> implements BaseFutureListener<F>
{
	final private static Logger logger = LoggerFactory.getLogger(BaseFutureAdapter.class);
	public void exceptionCaught(final Throwable t) throws Exception
	{
		logger.error("exception in "+getClass().getName());
		t.printStackTrace();
	}
}
