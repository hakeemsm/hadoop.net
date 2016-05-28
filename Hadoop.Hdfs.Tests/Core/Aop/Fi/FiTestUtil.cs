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
using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.FI
{
	/// <summary>Test Utilities</summary>
	public class FiTestUtil
	{
		/// <summary>Logging</summary>
		public static readonly Log Log = LogFactory.GetLog(typeof(FiTestUtil));

		private sealed class _ThreadLocal_33 : ThreadLocal<Random>
		{
			public _ThreadLocal_33()
			{
			}

			protected override Random InitialValue()
			{
				Random r = new Random();
				long seed = r.NextLong();
				FiTestUtil.Log.Info(Sharpen.Thread.CurrentThread() + ": seed=" + seed);
				r.SetSeed(seed);
				return r;
			}
		}

		/// <summary>Random source</summary>
		public static readonly ThreadLocal<Random> Random = new _ThreadLocal_33();

		/// <summary>Return a random integer uniformly distributed over the interval [min,max).
		/// 	</summary>
		public static int NextRandomInt(int min, int max)
		{
			int d = max - min;
			if (d <= 0)
			{
				throw new ArgumentException("d <= 0, min=" + min + ", max=" + max);
			}
			return d == 1 ? min : min + Random.Get().Next(d);
		}

		/// <summary>
		/// Return a random integer, with type long,
		/// uniformly distributed over the interval [min,max).
		/// </summary>
		/// <remarks>
		/// Return a random integer, with type long,
		/// uniformly distributed over the interval [min,max).
		/// Assume max - min &lt;= Integer.MAX_VALUE.
		/// </remarks>
		public static long NextRandomLong(long min, long max)
		{
			long d = max - min;
			if (d <= 0 || d > int.MaxValue)
			{
				throw new ArgumentException("d <= 0 || d > Integer.MAX_VALUE, min=" + min + ", max="
					 + max);
			}
			return d == 1 ? min : min + Random.Get().Next((int)d);
		}

		/// <summary>Return the method name of the callee.</summary>
		public static string GetMethodName()
		{
			StackTraceElement[] s = Sharpen.Thread.CurrentThread().GetStackTrace();
			return s[s.Length > 2 ? 2 : s.Length - 1].GetMethodName();
		}

		/// <summary>Sleep.</summary>
		/// <returns>true if sleep exits normally; false if InterruptedException.</returns>
		public static bool Sleep(long ms)
		{
			Log.Info("Sleep " + ms + " ms");
			try
			{
				Sharpen.Thread.Sleep(ms);
			}
			catch (Exception e)
			{
				Log.Info("Sleep is interrupted", e);
				return false;
			}
			return true;
		}

		/// <summary>Sleep a random number of milliseconds over the interval [min, max).</summary>
		/// <remarks>
		/// Sleep a random number of milliseconds over the interval [min, max).
		/// If there is an InterruptedException, re-throw it as a RuntimeException.
		/// </remarks>
		public static void Sleep(long min, long max)
		{
			long n = NextRandomLong(min, max);
			Log.Info(Sharpen.Thread.CurrentThread().GetName() + " sleeps for " + n + "ms");
			if (n > 0)
			{
				Sleep(n);
			}
		}

		/// <summary>Action interface</summary>
		public interface Action<T, E>
			where E : Exception
		{
			/// <summary>Run the action with the parameter.</summary>
			/// <exception cref="E"/>
			void Run(T parameter);
		}

		/// <summary>An ActionContainer contains at most one action.</summary>
		public class ActionContainer<T, E>
			where E : Exception
		{
			private IList<FiTestUtil.Action<T, E>> actionList = new AList<FiTestUtil.Action<T
				, E>>();

			/// <summary>Create an empty container.</summary>
			public ActionContainer()
			{
			}

			/// <summary>Set action.</summary>
			public virtual void Set(FiTestUtil.Action<T, E> a)
			{
				actionList.AddItem(a);
			}

			/// <summary>Run the action if it exists.</summary>
			/// <exception cref="E"/>
			public virtual void Run(T obj)
			{
				foreach (FiTestUtil.Action<T, E> action in actionList)
				{
					action.Run(obj);
				}
			}
		}

		/// <summary>Constraint interface</summary>
		public interface Constraint
		{
			/// <summary>Is this constraint satisfied?</summary>
			bool IsSatisfied();
		}

		/// <summary>Counting down, the constraint is satisfied if the count is one.</summary>
		public class CountdownConstraint : FiTestUtil.Constraint
		{
			private int count;

			/// <summary>Initialize the count.</summary>
			public CountdownConstraint(int count)
			{
				if (count < 1)
				{
					throw new ArgumentException(count + " = count < 1");
				}
				this.count = count;
			}

			/// <summary>Counting down, the constraint is satisfied if the count is zero.</summary>
			public virtual bool IsSatisfied()
			{
				if (count > 1)
				{
					count--;
					return false;
				}
				return true;
			}
		}

		/// <summary>An action is fired if all the constraints are satisfied.</summary>
		public class ConstraintSatisfactionAction<T, E> : FiTestUtil.Action<T, E>
			where E : Exception
		{
			private readonly FiTestUtil.Action<T, E> action;

			private readonly FiTestUtil.Constraint[] constraints;

			/// <summary>Constructor</summary>
			public ConstraintSatisfactionAction(FiTestUtil.Action<T, E> action, params FiTestUtil.Constraint
				[] constraints)
			{
				this.action = action;
				this.constraints = constraints;
			}

			/// <summary>Fire the action if all the constraints are satisfied.</summary>
			/// <remarks>
			/// Fire the action if all the constraints are satisfied.
			/// Short-circuit-and is used.
			/// </remarks>
			/// <exception cref="E"/>
			public void Run(T parameter)
			{
				foreach (FiTestUtil.Constraint c in constraints)
				{
					if (!c.IsSatisfied())
					{
						return;
					}
				}
				//all constraints are satisfied, fire the action
				action.Run(parameter);
			}
		}

		/// <summary>A MarkerConstraint is satisfied if it is marked.</summary>
		public class MarkerConstraint : FiTestUtil.Constraint
		{
			private readonly string name;

			private bool marked = false;

			/// <summary>Construct an object.</summary>
			public MarkerConstraint(string name)
			{
				this.name = name;
			}

			/// <summary>Set marker to be marked.</summary>
			public virtual void Mark()
			{
				marked = true;
				Log.Info("Marking this " + this);
			}

			/// <summary>Is the marker marked?</summary>
			public virtual bool IsSatisfied()
			{
				return marked;
			}

			/// <summary>
			/// <inheritDoc/>
			/// 
			/// </summary>
			public override string ToString()
			{
				return GetType().Name + "[" + name + ": " + marked + "]";
			}
		}
	}
}
