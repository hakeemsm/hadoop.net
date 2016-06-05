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


namespace Org.Apache.Hadoop.Service
{
	/// <summary>Implements the service state model.</summary>
	public class ServiceStateModel
	{
		/// <summary>
		/// Map of all valid state transitions
		/// [current] [proposed1, proposed2, ...]
		/// </summary>
		private static readonly bool[][] statemap = new bool[][] { new bool[] { false, true
			, false, true }, new bool[] { false, true, true, true }, new bool[] { false, false
			, true, true }, new bool[] { false, false, false, true } };

		/// <summary>The state of the service</summary>
		private volatile Service.STATE state;

		/// <summary>The name of the service: used in exceptions</summary>
		private string name;

		/// <summary>
		/// Create the service state model in the
		/// <see cref="STATE.Notinited"/>
		/// state.
		/// </summary>
		public ServiceStateModel(string name)
			: this(name, Service.STATE.Notinited)
		{
		}

		/// <summary>Create a service state model instance in the chosen state</summary>
		/// <param name="state">the starting state</param>
		public ServiceStateModel(string name, Service.STATE state)
		{
			//                uninited inited started stopped
			/* uninited  */
			/* inited    */
			/* started   */
			/* stopped   */
			this.state = state;
			this.name = name;
		}

		/// <summary>Query the service state.</summary>
		/// <remarks>Query the service state. This is a non-blocking operation.</remarks>
		/// <returns>the state</returns>
		public virtual Service.STATE GetState()
		{
			return state;
		}

		/// <summary>Query that the state is in a specific state</summary>
		/// <param name="proposed">proposed new state</param>
		/// <returns>the state</returns>
		public virtual bool IsInState(Service.STATE proposed)
		{
			return state.Equals(proposed);
		}

		/// <summary>Verify that that a service is in a given state.</summary>
		/// <param name="expectedState">the desired state</param>
		/// <exception cref="ServiceStateException">
		/// if the service state is different from
		/// the desired state
		/// </exception>
		public virtual void EnsureCurrentState(Service.STATE expectedState)
		{
			if (state != expectedState)
			{
				throw new ServiceStateException(name + ": for this operation, the " + "current service state must be "
					 + expectedState + " instead of " + state);
			}
		}

		/// <summary>Enter a state -thread safe.</summary>
		/// <param name="proposed">proposed new state</param>
		/// <returns>the original state</returns>
		/// <exception cref="ServiceStateException">if the transition is not permitted</exception>
		public virtual Service.STATE EnterState(Service.STATE proposed)
		{
			lock (this)
			{
				CheckStateTransition(name, state, proposed);
				Service.STATE oldState = state;
				//atomic write of the new state
				state = proposed;
				return oldState;
			}
		}

		/// <summary>
		/// Check that a state tansition is valid and
		/// throw an exception if not
		/// </summary>
		/// <param name="name">name of the service (can be null)</param>
		/// <param name="state">current state</param>
		/// <param name="proposed">proposed new state</param>
		public static void CheckStateTransition(string name, Service.STATE state, Service.STATE
			 proposed)
		{
			if (!IsValidStateTransition(state, proposed))
			{
				throw new ServiceStateException(name + " cannot enter state " + proposed + " from state "
					 + state);
			}
		}

		/// <summary>
		/// Is a state transition valid?
		/// There are no checks for current==proposed
		/// as that is considered a non-transition.
		/// </summary>
		/// <remarks>
		/// Is a state transition valid?
		/// There are no checks for current==proposed
		/// as that is considered a non-transition.
		/// using an array kills off all branch misprediction costs, at the expense
		/// of cache line misses.
		/// </remarks>
		/// <param name="current">current state</param>
		/// <param name="proposed">proposed new state</param>
		/// <returns>true if the transition to a new state is valid</returns>
		public static bool IsValidStateTransition(Service.STATE current, Service.STATE proposed
			)
		{
			bool[] row = statemap[current.GetValue()];
			return row[proposed.GetValue()];
		}

		/// <summary>return the state text as the toString() value</summary>
		/// <returns>the current state's description</returns>
		public override string ToString()
		{
			return (name.IsEmpty() ? string.Empty : ((name) + ": ")) + state.ToString();
		}
	}
}
