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
using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Counters
{
	/// <summary>The common counter group interface.</summary>
	/// <?/>
	public interface CounterGroupBase<T> : Writable, IEnumerable<T>
		where T : Counter
	{
		/// <summary>Get the internal name of the group</summary>
		/// <returns>the internal name</returns>
		string GetName();

		/// <summary>Get the display name of the group.</summary>
		/// <returns>the human readable name</returns>
		string GetDisplayName();

		/// <summary>Set the display name of the group</summary>
		/// <param name="displayName">of the group</param>
		void SetDisplayName(string displayName);

		/// <summary>Add a counter to this group.</summary>
		/// <param name="counter">to add</param>
		void AddCounter(T counter);

		/// <summary>Add a counter to this group</summary>
		/// <param name="name">of the counter</param>
		/// <param name="displayName">of the counter</param>
		/// <param name="value">of the counter</param>
		/// <returns>the counter</returns>
		T AddCounter(string name, string displayName, long value);

		/// <summary>Find a counter in the group.</summary>
		/// <param name="counterName">the name of the counter</param>
		/// <param name="displayName">the display name of the counter</param>
		/// <returns>the counter that was found or added</returns>
		T FindCounter(string counterName, string displayName);

		/// <summary>Find a counter in the group</summary>
		/// <param name="counterName">the name of the counter</param>
		/// <param name="create">create the counter if not found if true</param>
		/// <returns>the counter that was found or added or null if create is false</returns>
		T FindCounter(string counterName, bool create);

		/// <summary>Find a counter in the group.</summary>
		/// <param name="counterName">the name of the counter</param>
		/// <returns>the counter that was found or added</returns>
		T FindCounter(string counterName);

		/// <returns>the number of counters in this group.</returns>
		int Size();

		/// <summary>Increment all counters by a group of counters</summary>
		/// <param name="rightGroup">the group to be added to this group</param>
		void IncrAllCounters(CounterGroupBase<T> rightGroup);

		[InterfaceAudience.Private]
		CounterGroupBase<T> GetUnderlyingGroup();
	}
}
