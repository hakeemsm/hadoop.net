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
using System.Collections.Generic;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Service
{
	/// <summary>
	/// A state change listener that logs the number of state change events received,
	/// and the last state invoked.
	/// </summary>
	/// <remarks>
	/// A state change listener that logs the number of state change events received,
	/// and the last state invoked.
	/// It can be configured to fail during a state change event
	/// </remarks>
	public class BreakableStateChangeListener : ServiceStateChangeListener
	{
		private readonly string name;

		private int eventCount;

		private int failureCount;

		private Org.Apache.Hadoop.Service.Service lastService;

		private Service.STATE lastState = Service.STATE.Notinited;

		private Service.STATE failingState = Service.STATE.Notinited;

		private IList<Service.STATE> stateEventList = new AList<Service.STATE>(4);

		public BreakableStateChangeListener()
			: this("BreakableStateChangeListener")
		{
		}

		public BreakableStateChangeListener(string name)
		{
			//no callbacks are ever received for this event, so it
			//can be used as an 'undefined'.
			this.name = name;
		}

		public virtual void StateChanged(Org.Apache.Hadoop.Service.Service service)
		{
			lock (this)
			{
				eventCount++;
				lastService = service;
				lastState = service.GetServiceState();
				stateEventList.AddItem(lastState);
				if (lastState == failingState)
				{
					failureCount++;
					throw new BreakableService.BrokenLifecycleEvent(service, "Failure entering " + lastState
						 + " for " + service.GetName());
				}
			}
		}

		public virtual int GetEventCount()
		{
			lock (this)
			{
				return eventCount;
			}
		}

		public virtual Org.Apache.Hadoop.Service.Service GetLastService()
		{
			lock (this)
			{
				return lastService;
			}
		}

		public virtual Service.STATE GetLastState()
		{
			lock (this)
			{
				return lastState;
			}
		}

		public virtual void SetFailingState(Service.STATE failingState)
		{
			lock (this)
			{
				this.failingState = failingState;
			}
		}

		public virtual int GetFailureCount()
		{
			lock (this)
			{
				return failureCount;
			}
		}

		public virtual IList<Service.STATE> GetStateEventList()
		{
			return stateEventList;
		}

		public override string ToString()
		{
			lock (this)
			{
				string s = name + " - event count = " + eventCount + " last state " + lastState;
				StringBuilder history = new StringBuilder(stateEventList.Count * 10);
				foreach (Service.STATE state in stateEventList)
				{
					history.Append(state).Append(" ");
				}
				return s + " [ " + history + "]";
			}
		}
	}
}
