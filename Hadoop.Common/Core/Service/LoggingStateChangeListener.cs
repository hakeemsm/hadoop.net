/*
* Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Service
{
	/// <summary>This is a state change listener that logs events at INFO level</summary>
	public class LoggingStateChangeListener : ServiceStateChangeListener
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Service.LoggingStateChangeListener
			));

		private readonly Log log;

		/// <summary>Log events to the given log</summary>
		/// <param name="log">destination for events</param>
		public LoggingStateChangeListener(Log log)
		{
			//force an NPE if a null log came in
			log.IsDebugEnabled();
			this.log = log;
		}

		/// <summary>Log events to the static log for this class</summary>
		public LoggingStateChangeListener()
			: this(Log)
		{
		}

		/// <summary>Callback for a state change event: log it</summary>
		/// <param name="service">the service that has changed.</param>
		public virtual void StateChanged(Org.Apache.Hadoop.Service.Service service)
		{
			log.Info("Entry to state " + service.GetServiceState() + " for " + service.GetName
				());
		}
	}
}
