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
using Sharpen;

namespace Org.Apache.Hadoop.Service
{
	/// <summary>
	/// A serializable lifecycle event: the time a state
	/// transition occurred, and what state was entered.
	/// </summary>
	[System.Serializable]
	public class LifecycleEvent
	{
		private const long serialVersionUID = 1648576996238247836L;

		/// <summary>Local time in milliseconds when the event occurred</summary>
		public long time;

		/// <summary>new state</summary>
		public Service.STATE state;
	}
}
