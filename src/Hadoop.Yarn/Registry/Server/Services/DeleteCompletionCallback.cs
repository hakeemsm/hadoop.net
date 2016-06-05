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
using Org.Apache.Hadoop.Registry.Server.Integration;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Registry.Server.Services
{
	/// <summary>Curator callback for delete operations completing.</summary>
	/// <remarks>
	/// Curator callback for delete operations completing.
	/// <p>
	/// This callback logs at debug and increments the event counter.
	/// </remarks>
	public class DeleteCompletionCallback : BackgroundCallback
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(RMRegistryOperationsService
			));

		private AtomicInteger events = new AtomicInteger(0);

		/// <exception cref="System.Exception"/>
		public virtual void ProcessResult(CuratorFramework client, CuratorEvent @event)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Delete event {}", @event);
			}
			events.IncrementAndGet();
		}

		/// <summary>Get the number of deletion events</summary>
		/// <returns>the count of events</returns>
		public virtual int GetEventCount()
		{
			return events.Get();
		}
	}
}
