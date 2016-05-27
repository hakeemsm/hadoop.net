/*
* MetricsContext.java
*
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
using System.Collections.Generic;
using Org.Apache.Hadoop.Metrics.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics
{
	/// <summary>The main interface to the metrics package.</summary>
	public abstract class MetricsContext
	{
		/// <summary>Default period in seconds at which data is sent to the metrics system.</summary>
		public const int DefaultPeriod = 5;

		/// <summary>Initialize this context.</summary>
		/// <param name="contextName">The given name for this context</param>
		/// <param name="factory">The creator of this context</param>
		public abstract void Init(string contextName, ContextFactory factory);

		/// <summary>Returns the context name.</summary>
		/// <returns>the context name</returns>
		public abstract string GetContextName();

		/// <summary>
		/// Starts or restarts monitoring, the emitting of metrics records as they are
		/// updated.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void StartMonitoring();

		/// <summary>Stops monitoring.</summary>
		/// <remarks>
		/// Stops monitoring.  This does not free any data that the implementation
		/// may have buffered for sending at the next timer event. It
		/// is OK to call <code>startMonitoring()</code> again after calling
		/// this.
		/// </remarks>
		/// <seealso cref="Close()"/>
		public abstract void StopMonitoring();

		/// <summary>Returns true if monitoring is currently in progress.</summary>
		public abstract bool IsMonitoring();

		/// <summary>
		/// Stops monitoring and also frees any buffered data, returning this
		/// object to its initial state.
		/// </summary>
		public abstract void Close();

		/// <summary>Creates a new MetricsRecord instance with the given <code>recordName</code>.
		/// 	</summary>
		/// <remarks>
		/// Creates a new MetricsRecord instance with the given <code>recordName</code>.
		/// Throws an exception if the metrics implementation is configured with a fixed
		/// set of record names and <code>recordName</code> is not in that set.
		/// </remarks>
		/// <param name="recordName">the name of the record</param>
		/// <exception cref="MetricsException">if recordName conflicts with configuration data
		/// 	</exception>
		public abstract MetricsRecord CreateRecord(string recordName);

		/// <summary>
		/// Registers a callback to be called at regular time intervals, as
		/// determined by the implementation-class specific configuration.
		/// </summary>
		/// <param name="updater">
		/// object to be run periodically; it should updated
		/// some metrics records and then return
		/// </param>
		public abstract void RegisterUpdater(Updater updater);

		/// <summary>Removes a callback, if it exists.</summary>
		/// <param name="updater">object to be removed from the callback list</param>
		public abstract void UnregisterUpdater(Updater updater);

		/// <summary>Returns the timer period.</summary>
		public abstract int GetPeriod();

		/// <summary>Retrieves all the records managed by this MetricsContext.</summary>
		/// <remarks>
		/// Retrieves all the records managed by this MetricsContext.
		/// Useful for monitoring systems that are polling-based.
		/// </remarks>
		/// <returns>A non-null map from all record names to the records managed.</returns>
		public abstract IDictionary<string, ICollection<OutputRecord>> GetAllRecords();
	}

	public static class MetricsContextConstants
	{
	}
}
