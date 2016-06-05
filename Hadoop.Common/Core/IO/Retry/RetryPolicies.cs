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
using System.IO;
using System.Net.Sockets;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;


namespace Org.Apache.Hadoop.IO.Retry
{
	/// <summary>
	/// <p>
	/// A collection of useful implementations of
	/// <see cref="RetryPolicy"/>
	/// .
	/// </p>
	/// </summary>
	public class RetryPolicies
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(RetryPolicies));

		private sealed class _ThreadLocal_50 : ThreadLocal<Random>
		{
			public _ThreadLocal_50()
			{
			}

			protected override Random InitialValue()
			{
				return new Random();
			}
		}

		private static ThreadLocal<Random> Random = new _ThreadLocal_50();

		/// <summary>
		/// <p>
		/// Try once, and fail by re-throwing the exception.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Try once, and fail by re-throwing the exception.
		/// This corresponds to having no retry mechanism in place.
		/// </p>
		/// </remarks>
		public static readonly RetryPolicy TryOnceThenFail = new RetryPolicies.TryOnceThenFail
			();

		/// <summary>
		/// <p>
		/// Keep trying forever.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Keep trying forever.
		/// </p>
		/// </remarks>
		public static readonly RetryPolicy RetryForever = new RetryPolicies.RetryForever(
			);

		/// <summary>
		/// <p>
		/// Keep trying a limited number of times, waiting a fixed time between attempts,
		/// and then fail by re-throwing the exception.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Keep trying a limited number of times, waiting a fixed time between attempts,
		/// and then fail by re-throwing the exception.
		/// </p>
		/// </remarks>
		public static RetryPolicy RetryUpToMaximumCountWithFixedSleep(int maxRetries, long
			 sleepTime, TimeUnit timeUnit)
		{
			return new RetryPolicies.RetryUpToMaximumCountWithFixedSleep(maxRetries, sleepTime
				, timeUnit);
		}

		/// <summary>
		/// <p>
		/// Keep trying for a maximum time, waiting a fixed time between attempts,
		/// and then fail by re-throwing the exception.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Keep trying for a maximum time, waiting a fixed time between attempts,
		/// and then fail by re-throwing the exception.
		/// </p>
		/// </remarks>
		public static RetryPolicy RetryUpToMaximumTimeWithFixedSleep(long maxTime, long sleepTime
			, TimeUnit timeUnit)
		{
			return new RetryPolicies.RetryUpToMaximumTimeWithFixedSleep(maxTime, sleepTime, timeUnit
				);
		}

		/// <summary>
		/// <p>
		/// Keep trying a limited number of times, waiting a growing amount of time between attempts,
		/// and then fail by re-throwing the exception.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Keep trying a limited number of times, waiting a growing amount of time between attempts,
		/// and then fail by re-throwing the exception.
		/// The time between attempts is <code>sleepTime</code> mutliplied by the number of tries so far.
		/// </p>
		/// </remarks>
		public static RetryPolicy RetryUpToMaximumCountWithProportionalSleep(int maxRetries
			, long sleepTime, TimeUnit timeUnit)
		{
			return new RetryPolicies.RetryUpToMaximumCountWithProportionalSleep(maxRetries, sleepTime
				, timeUnit);
		}

		/// <summary>
		/// <p>
		/// Keep trying a limited number of times, waiting a growing amount of time between attempts,
		/// and then fail by re-throwing the exception.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Keep trying a limited number of times, waiting a growing amount of time between attempts,
		/// and then fail by re-throwing the exception.
		/// The time between attempts is <code>sleepTime</code> mutliplied by a random
		/// number in the range of [0, 2 to the number of retries)
		/// </p>
		/// </remarks>
		public static RetryPolicy ExponentialBackoffRetry(int maxRetries, long sleepTime, 
			TimeUnit timeUnit)
		{
			return new RetryPolicies.ExponentialBackoffRetry(maxRetries, sleepTime, timeUnit);
		}

		/// <summary>
		/// <p>
		/// Set a default policy with some explicit handlers for specific exceptions.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Set a default policy with some explicit handlers for specific exceptions.
		/// </p>
		/// </remarks>
		public static RetryPolicy RetryByException(RetryPolicy defaultPolicy, IDictionary
			<Type, RetryPolicy> exceptionToPolicyMap)
		{
			return new RetryPolicies.ExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap
				);
		}

		/// <summary>
		/// <p>
		/// A retry policy for RemoteException
		/// Set a default policy with some explicit handlers for specific exceptions.
		/// </summary>
		/// <remarks>
		/// <p>
		/// A retry policy for RemoteException
		/// Set a default policy with some explicit handlers for specific exceptions.
		/// </p>
		/// </remarks>
		public static RetryPolicy RetryByRemoteException(RetryPolicy defaultPolicy, IDictionary
			<Type, RetryPolicy> exceptionToPolicyMap)
		{
			return new RetryPolicies.RemoteExceptionDependentRetry(defaultPolicy, exceptionToPolicyMap
				);
		}

		public static RetryPolicy FailoverOnNetworkException(int maxFailovers)
		{
			return FailoverOnNetworkException(TryOnceThenFail, maxFailovers);
		}

		public static RetryPolicy FailoverOnNetworkException(RetryPolicy fallbackPolicy, 
			int maxFailovers)
		{
			return FailoverOnNetworkException(fallbackPolicy, maxFailovers, 0, 0);
		}

		public static RetryPolicy FailoverOnNetworkException(RetryPolicy fallbackPolicy, 
			int maxFailovers, long delayMillis, long maxDelayBase)
		{
			return new RetryPolicies.FailoverOnNetworkExceptionRetry(fallbackPolicy, maxFailovers
				, delayMillis, maxDelayBase);
		}

		public static RetryPolicy FailoverOnNetworkException(RetryPolicy fallbackPolicy, 
			int maxFailovers, int maxRetries, long delayMillis, long maxDelayBase)
		{
			return new RetryPolicies.FailoverOnNetworkExceptionRetry(fallbackPolicy, maxFailovers
				, maxRetries, delayMillis, maxDelayBase);
		}

		internal class TryOnceThenFail : RetryPolicy
		{
			/// <exception cref="System.Exception"/>
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
				 failovers, bool isIdempotentOrAtMostOnce)
			{
				return RetryPolicy.RetryAction.Fail;
			}
		}

		internal class RetryForever : RetryPolicy
		{
			/// <exception cref="System.Exception"/>
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
				 failovers, bool isIdempotentOrAtMostOnce)
			{
				return RetryPolicy.RetryAction.Retry;
			}
		}

		/// <summary>Retry up to maxRetries.</summary>
		/// <remarks>
		/// Retry up to maxRetries.
		/// The actual sleep time of the n-th retry is f(n, sleepTime),
		/// where f is a function provided by the subclass implementation.
		/// The object of the subclasses should be immutable;
		/// otherwise, the subclass must override hashCode(), equals(..) and toString().
		/// </remarks>
		internal abstract class RetryLimited : RetryPolicy
		{
			internal readonly int maxRetries;

			internal readonly long sleepTime;

			internal readonly TimeUnit timeUnit;

			private string myString;

			internal RetryLimited(int maxRetries, long sleepTime, TimeUnit timeUnit)
			{
				if (maxRetries < 0)
				{
					throw new ArgumentException("maxRetries = " + maxRetries + " < 0");
				}
				if (sleepTime < 0)
				{
					throw new ArgumentException("sleepTime = " + sleepTime + " < 0");
				}
				this.maxRetries = maxRetries;
				this.sleepTime = sleepTime;
				this.timeUnit = timeUnit;
			}

			/// <exception cref="System.Exception"/>
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
				 failovers, bool isIdempotentOrAtMostOnce)
			{
				if (retries >= maxRetries)
				{
					return RetryPolicy.RetryAction.Fail;
				}
				return new RetryPolicy.RetryAction(RetryPolicy.RetryAction.RetryDecision.Retry, timeUnit
					.ToMillis(CalculateSleepTime(retries)));
			}

			protected internal abstract long CalculateSleepTime(int retries);

			public override int GetHashCode()
			{
				return ToString().GetHashCode();
			}

			public override bool Equals(object that)
			{
				if (this == that)
				{
					return true;
				}
				else
				{
					if (that == null || this.GetType() != that.GetType())
					{
						return false;
					}
				}
				return this.ToString().Equals(that.ToString());
			}

			public override string ToString()
			{
				if (myString == null)
				{
					myString = GetType().Name + "(maxRetries=" + maxRetries + ", sleepTime=" + sleepTime
						 + " " + timeUnit + ")";
				}
				return myString;
			}
		}

		internal class RetryUpToMaximumCountWithFixedSleep : RetryPolicies.RetryLimited
		{
			public RetryUpToMaximumCountWithFixedSleep(int maxRetries, long sleepTime, TimeUnit
				 timeUnit)
				: base(maxRetries, sleepTime, timeUnit)
			{
			}

			protected internal override long CalculateSleepTime(int retries)
			{
				return sleepTime;
			}
		}

		internal class RetryUpToMaximumTimeWithFixedSleep : RetryPolicies.RetryUpToMaximumCountWithFixedSleep
		{
			public RetryUpToMaximumTimeWithFixedSleep(long maxTime, long sleepTime, TimeUnit 
				timeUnit)
				: base((int)(maxTime / sleepTime), sleepTime, timeUnit)
			{
			}
		}

		internal class RetryUpToMaximumCountWithProportionalSleep : RetryPolicies.RetryLimited
		{
			public RetryUpToMaximumCountWithProportionalSleep(int maxRetries, long sleepTime, 
				TimeUnit timeUnit)
				: base(maxRetries, sleepTime, timeUnit)
			{
			}

			protected internal override long CalculateSleepTime(int retries)
			{
				return sleepTime * (retries + 1);
			}
		}

		/// <summary>
		/// Given pairs of number of retries and sleep time (n0, t0), (n1, t1), ...,
		/// the first n0 retries sleep t0 milliseconds on average,
		/// the following n1 retries sleep t1 milliseconds on average, and so on.
		/// </summary>
		/// <remarks>
		/// Given pairs of number of retries and sleep time (n0, t0), (n1, t1), ...,
		/// the first n0 retries sleep t0 milliseconds on average,
		/// the following n1 retries sleep t1 milliseconds on average, and so on.
		/// For all the sleep, the actual sleep time is randomly uniform distributed
		/// in the close interval [0.5t, 1.5t], where t is the sleep time specified.
		/// The objects of this class are immutable.
		/// </remarks>
		public class MultipleLinearRandomRetry : RetryPolicy
		{
			/// <summary>Pairs of numRetries and sleepSeconds</summary>
			public class Pair
			{
				internal readonly int numRetries;

				internal readonly int sleepMillis;

				public Pair(int numRetries, int sleepMillis)
				{
					if (numRetries < 0)
					{
						throw new ArgumentException("numRetries = " + numRetries + " < 0");
					}
					if (sleepMillis < 0)
					{
						throw new ArgumentException("sleepMillis = " + sleepMillis + " < 0");
					}
					this.numRetries = numRetries;
					this.sleepMillis = sleepMillis;
				}

				public override string ToString()
				{
					return numRetries + "x" + sleepMillis + "ms";
				}
			}

			private readonly IList<RetryPolicies.MultipleLinearRandomRetry.Pair> pairs;

			private string myString;

			public MultipleLinearRandomRetry(IList<RetryPolicies.MultipleLinearRandomRetry.Pair
				> pairs)
			{
				if (pairs == null || pairs.IsEmpty())
				{
					throw new ArgumentException("pairs must be neither null nor empty.");
				}
				this.pairs = Collections.UnmodifiableList(pairs);
			}

			/// <exception cref="System.Exception"/>
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int curRetry, int
				 failovers, bool isIdempotentOrAtMostOnce)
			{
				RetryPolicies.MultipleLinearRandomRetry.Pair p = SearchPair(curRetry);
				if (p == null)
				{
					//no more retries.
					return RetryPolicy.RetryAction.Fail;
				}
				//calculate sleep time and return.
				double ratio = Random.Get().NextDouble() + 0.5;
				//0.5 <= ratio <=1.5
				long sleepTime = Math.Round(p.sleepMillis * ratio);
				return new RetryPolicy.RetryAction(RetryPolicy.RetryAction.RetryDecision.Retry, sleepTime
					);
			}

			/// <summary>Given the current number of retry, search the corresponding pair.</summary>
			/// <returns>
			/// the corresponding pair,
			/// or null if the current number of retry &gt; maximum number of retry.
			/// </returns>
			private RetryPolicies.MultipleLinearRandomRetry.Pair SearchPair(int curRetry)
			{
				int i = 0;
				for (; i < pairs.Count && curRetry > pairs[i].numRetries; i++)
				{
					curRetry -= pairs[i].numRetries;
				}
				return i == pairs.Count ? null : pairs[i];
			}

			public override int GetHashCode()
			{
				return ToString().GetHashCode();
			}

			public override bool Equals(object that)
			{
				if (this == that)
				{
					return true;
				}
				else
				{
					if (that == null || this.GetType() != that.GetType())
					{
						return false;
					}
				}
				return this.ToString().Equals(that.ToString());
			}

			public override string ToString()
			{
				if (myString == null)
				{
					myString = GetType().Name + pairs;
				}
				return myString;
			}

			/// <summary>Parse the given string as a MultipleLinearRandomRetry object.</summary>
			/// <remarks>
			/// Parse the given string as a MultipleLinearRandomRetry object.
			/// The format of the string is "t_1, n_1, t_2, n_2, ...",
			/// where t_i and n_i are the i-th pair of sleep time and number of retires.
			/// Note that the white spaces in the string are ignored.
			/// </remarks>
			/// <returns>the parsed object, or null if the parsing fails.</returns>
			public static RetryPolicies.MultipleLinearRandomRetry ParseCommaSeparatedString(string
				 s)
			{
				string[] elements = s.Split(",");
				if (elements.Length == 0)
				{
					Log.Warn("Illegal value: there is no element in \"" + s + "\".");
					return null;
				}
				if (elements.Length % 2 != 0)
				{
					Log.Warn("Illegal value: the number of elements in \"" + s + "\" is " + elements.
						Length + " but an even number of elements is expected.");
					return null;
				}
				IList<RetryPolicies.MultipleLinearRandomRetry.Pair> pairs = new AList<RetryPolicies.MultipleLinearRandomRetry.Pair
					>();
				for (int i = 0; i < elements.Length; )
				{
					//parse the i-th sleep-time
					int sleep = ParsePositiveInt(elements, i++, s);
					if (sleep == -1)
					{
						return null;
					}
					//parse fails
					//parse the i-th number-of-retries
					int retries = ParsePositiveInt(elements, i++, s);
					if (retries == -1)
					{
						return null;
					}
					//parse fails
					pairs.AddItem(new RetryPolicies.MultipleLinearRandomRetry.Pair(retries, sleep));
				}
				return new RetryPolicies.MultipleLinearRandomRetry(pairs);
			}

			/// <summary>Parse the i-th element as an integer.</summary>
			/// <returns>
			/// -1 if the parsing fails or the parsed value &lt;= 0;
			/// otherwise, return the parsed value.
			/// </returns>
			private static int ParsePositiveInt(string[] elements, int i, string originalString
				)
			{
				string s = elements[i].Trim();
				int n;
				try
				{
					n = System.Convert.ToInt32(s);
				}
				catch (FormatException nfe)
				{
					Log.Warn("Failed to parse \"" + s + "\", which is the index " + i + " element in \""
						 + originalString + "\"", nfe);
					return -1;
				}
				if (n <= 0)
				{
					Log.Warn("The value " + n + " <= 0: it is parsed from the string \"" + s + "\" which is the index "
						 + i + " element in \"" + originalString + "\"");
					return -1;
				}
				return n;
			}
		}

		internal class ExceptionDependentRetry : RetryPolicy
		{
			internal RetryPolicy defaultPolicy;

			internal IDictionary<Type, RetryPolicy> exceptionToPolicyMap;

			public ExceptionDependentRetry(RetryPolicy defaultPolicy, IDictionary<Type, RetryPolicy
				> exceptionToPolicyMap)
			{
				this.defaultPolicy = defaultPolicy;
				this.exceptionToPolicyMap = exceptionToPolicyMap;
			}

			/// <exception cref="System.Exception"/>
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
				 failovers, bool isIdempotentOrAtMostOnce)
			{
				RetryPolicy policy = exceptionToPolicyMap[e.GetType()];
				if (policy == null)
				{
					policy = defaultPolicy;
				}
				return policy.ShouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
			}
		}

		internal class RemoteExceptionDependentRetry : RetryPolicy
		{
			internal RetryPolicy defaultPolicy;

			internal IDictionary<string, RetryPolicy> exceptionNameToPolicyMap;

			public RemoteExceptionDependentRetry(RetryPolicy defaultPolicy, IDictionary<Type, 
				RetryPolicy> exceptionToPolicyMap)
			{
				this.defaultPolicy = defaultPolicy;
				this.exceptionNameToPolicyMap = new Dictionary<string, RetryPolicy>();
				foreach (KeyValuePair<Type, RetryPolicy> e in exceptionToPolicyMap)
				{
					exceptionNameToPolicyMap[e.Key.FullName] = e.Value;
				}
			}

			/// <exception cref="System.Exception"/>
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
				 failovers, bool isIdempotentOrAtMostOnce)
			{
				RetryPolicy policy = null;
				if (e is RemoteException)
				{
					policy = exceptionNameToPolicyMap[((RemoteException)e).GetClassName()];
				}
				if (policy == null)
				{
					policy = defaultPolicy;
				}
				return policy.ShouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
			}
		}

		internal class ExponentialBackoffRetry : RetryPolicies.RetryLimited
		{
			public ExponentialBackoffRetry(int maxRetries, long sleepTime, TimeUnit timeUnit)
				: base(maxRetries, sleepTime, timeUnit)
			{
				if (maxRetries < 0)
				{
					throw new ArgumentException("maxRetries = " + maxRetries + " < 0");
				}
				else
				{
					if (maxRetries >= long.Size - 1)
					{
						//calculateSleepTime may overflow. 
						throw new ArgumentException("maxRetries = " + maxRetries + " >= " + (long.Size - 
							1));
					}
				}
			}

			protected internal override long CalculateSleepTime(int retries)
			{
				return CalculateExponentialTime(sleepTime, retries + 1);
			}
		}

		/// <summary>
		/// Fail over and retry in the case of:
		/// Remote StandbyException (server is up, but is not the active server)
		/// Immediate socket exceptions (e.g.
		/// </summary>
		/// <remarks>
		/// Fail over and retry in the case of:
		/// Remote StandbyException (server is up, but is not the active server)
		/// Immediate socket exceptions (e.g. no route to host, econnrefused)
		/// Socket exceptions after initial connection when operation is idempotent
		/// The first failover is immediate, while all subsequent failovers wait an
		/// exponentially-increasing random amount of time.
		/// Fail immediately in the case of:
		/// Socket exceptions after initial connection when operation is not idempotent
		/// Fall back on underlying retry policy otherwise.
		/// </remarks>
		internal class FailoverOnNetworkExceptionRetry : RetryPolicy
		{
			private RetryPolicy fallbackPolicy;

			private int maxFailovers;

			private int maxRetries;

			private long delayMillis;

			private long maxDelayBase;

			public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy, int maxFailovers
				)
				: this(fallbackPolicy, maxFailovers, 0, 0, 0)
			{
			}

			public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy, int maxFailovers
				, long delayMillis, long maxDelayBase)
				: this(fallbackPolicy, maxFailovers, 0, delayMillis, maxDelayBase)
			{
			}

			public FailoverOnNetworkExceptionRetry(RetryPolicy fallbackPolicy, int maxFailovers
				, int maxRetries, long delayMillis, long maxDelayBase)
			{
				this.fallbackPolicy = fallbackPolicy;
				this.maxFailovers = maxFailovers;
				this.maxRetries = maxRetries;
				this.delayMillis = delayMillis;
				this.maxDelayBase = maxDelayBase;
			}

			/// <returns>
			/// 0 if this is our first failover/retry (i.e., retry immediately),
			/// sleep exponentially otherwise
			/// </returns>
			private long GetFailoverOrRetrySleepTime(int times)
			{
				return times == 0 ? 0 : CalculateExponentialTime(delayMillis, times, maxDelayBase
					);
			}

			/// <exception cref="System.Exception"/>
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
				 failovers, bool isIdempotentOrAtMostOnce)
			{
				if (failovers >= maxFailovers)
				{
					return new RetryPolicy.RetryAction(RetryPolicy.RetryAction.RetryDecision.Fail, 0, 
						"failovers (" + failovers + ") exceeded maximum allowed (" + maxFailovers + ")");
				}
				if (retries - failovers > maxRetries)
				{
					return new RetryPolicy.RetryAction(RetryPolicy.RetryAction.RetryDecision.Fail, 0, 
						"retries (" + retries + ") exceeded maximum allowed (" + maxRetries + ")");
				}
				if (e is ConnectException || e is NoRouteToHostException || e is UnknownHostException
					 || e is StandbyException || e is ConnectTimeoutException || IsWrappedStandbyException
					(e))
				{
					return new RetryPolicy.RetryAction(RetryPolicy.RetryAction.RetryDecision.FailoverAndRetry
						, GetFailoverOrRetrySleepTime(failovers));
				}
				else
				{
					if (e is RetriableException || GetWrappedRetriableException(e) != null)
					{
						// RetriableException or RetriableException wrapped 
						return new RetryPolicy.RetryAction(RetryPolicy.RetryAction.RetryDecision.Retry, GetFailoverOrRetrySleepTime
							(retries));
					}
					else
					{
						if (e is SocketException || (e is IOException && !(e is RemoteException)))
						{
							if (isIdempotentOrAtMostOnce)
							{
								return RetryPolicy.RetryAction.FailoverAndRetry;
							}
							else
							{
								return new RetryPolicy.RetryAction(RetryPolicy.RetryAction.RetryDecision.Fail, 0, 
									"the invoked method is not idempotent, and unable to determine " + "whether it was invoked"
									);
							}
						}
						else
						{
							return fallbackPolicy.ShouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce
								);
						}
					}
				}
			}
		}

		/// <summary>
		/// Return a value which is <code>time</code> increasing exponentially as a
		/// function of <code>retries</code>, +/- 0%-50% of that value, chosen
		/// randomly.
		/// </summary>
		/// <param name="time">the base amount of time to work with</param>
		/// <param name="retries">the number of retries that have so occurred so far</param>
		/// <param name="cap">value at which to cap the base sleep time</param>
		/// <returns>an amount of time to sleep</returns>
		private static long CalculateExponentialTime(long time, int retries, long cap)
		{
			long baseTime = Math.Min(time * (1L << retries), cap);
			return (long)(baseTime * (Random.Get().NextDouble() + 0.5));
		}

		private static long CalculateExponentialTime(long time, int retries)
		{
			return CalculateExponentialTime(time, retries, long.MaxValue);
		}

		private static bool IsWrappedStandbyException(Exception e)
		{
			if (!(e is RemoteException))
			{
				return false;
			}
			Exception unwrapped = ((RemoteException)e).UnwrapRemoteException(typeof(StandbyException
				));
			return unwrapped is StandbyException;
		}

		internal static RetriableException GetWrappedRetriableException(Exception e)
		{
			if (!(e is RemoteException))
			{
				return null;
			}
			Exception unwrapped = ((RemoteException)e).UnwrapRemoteException(typeof(RetriableException
				));
			return unwrapped is RetriableException ? (RetriableException)unwrapped : null;
		}
	}
}
