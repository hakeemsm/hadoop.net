/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
using Org.Apache.Curator.Framework;
using Org.Apache.Curator.Framework.Api;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Client.Impl
{
	/// <summary>
	/// This is a little event catcher for curator asynchronous
	/// operations.
	/// </summary>
	public class CuratorEventCatcher : BackgroundCallback
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(CuratorEventCatcher
			));

		public readonly BlockingQueue<CuratorEvent> events = new LinkedBlockingQueue<CuratorEvent
			>(1);

		private readonly AtomicInteger eventCounter = new AtomicInteger(0);

		/// <exception cref="System.Exception"/>
		public virtual void ProcessResult(CuratorFramework client, CuratorEvent @event)
		{
			Log.Info("received {}", @event);
			eventCounter.IncrementAndGet();
			events.Put(@event);
		}

		public virtual int GetCount()
		{
			return eventCounter.Get();
		}

		/// <summary>Blocking operation to take the first event off the queue</summary>
		/// <returns>the first event on the queue, when it arrives</returns>
		/// <exception cref="System.Exception">if interrupted</exception>
		public virtual CuratorEvent Take()
		{
			return events.Take();
		}
	}
}
