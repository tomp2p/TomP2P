/*
 * Copyright 2012 Thomas Bocek
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
import net.tomp2p.futures.BaseFuture.FutureType;
import net.tomp2p.message.Message;

/**
 * The communication future success evaluator returns OK if the communication was
 * successful, otherwise it returns FAILED. This evaluation does not care if
 * e.g. an object was not found on an other peer. This is because the peer
 * successfully reported that the element is not present. If an other evaluation
 * scheme is necessary, provide your own.
 * 
 * @author Thomas Bocek
 */
public class FutureSuccessEvaluatorCommunication implements FutureSuccessEvaluator
{
	@Override
	public FutureType evaluate(Message requestMessage, Message responseMessage)
	{
		return (responseMessage.isOk() || responseMessage.isNotOk()) ? FutureType.OK
				: FutureType.FAILED;
	}
}
