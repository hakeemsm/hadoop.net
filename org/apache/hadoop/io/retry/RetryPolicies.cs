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

namespace org.apache.hadoop.io.retry
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
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.retry.RetryPolicies
			)));

		private sealed class _ThreadLocal_50 : java.lang.ThreadLocal<java.util.Random>
		{
			public _ThreadLocal_50()
			{
			}

			protected override java.util.Random initialValue()
			{
				return new java.util.Random();
			}
		}

		private static java.lang.ThreadLocal<java.util.Random> RANDOM = new _ThreadLocal_50
			();

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
		public static readonly org.apache.hadoop.io.retry.RetryPolicy TRY_ONCE_THEN_FAIL = 
			new org.apache.hadoop.io.retry.RetryPolicies.TryOnceThenFail();

		/// <summary>
		/// <p>
		/// Keep trying forever.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Keep trying forever.
		/// </p>
		/// </remarks>
		public static readonly org.apache.hadoop.io.retry.RetryPolicy RETRY_FOREVER = new 
			org.apache.hadoop.io.retry.RetryPolicies.RetryForever();

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
		public static org.apache.hadoop.io.retry.RetryPolicy retryUpToMaximumCountWithFixedSleep
			(int maxRetries, long sleepTime, java.util.concurrent.TimeUnit timeUnit)
		{
			return new org.apache.hadoop.io.retry.RetryPolicies.RetryUpToMaximumCountWithFixedSleep
				(maxRetries, sleepTime, timeUnit);
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
		public static org.apache.hadoop.io.retry.RetryPolicy retryUpToMaximumTimeWithFixedSleep
			(long maxTime, long sleepTime, java.util.concurrent.TimeUnit timeUnit)
		{
			return new org.apache.hadoop.io.retry.RetryPolicies.RetryUpToMaximumTimeWithFixedSleep
				(maxTime, sleepTime, timeUnit);
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
		public static org.apache.hadoop.io.retry.RetryPolicy retryUpToMaximumCountWithProportionalSleep
			(int maxRetries, long sleepTime, java.util.concurrent.TimeUnit timeUnit)
		{
			return new org.apache.hadoop.io.retry.RetryPolicies.RetryUpToMaximumCountWithProportionalSleep
				(maxRetries, sleepTime, timeUnit);
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
		public static org.apache.hadoop.io.retry.RetryPolicy exponentialBackoffRetry(int 
			maxRetries, long sleepTime, java.util.concurrent.TimeUnit timeUnit)
		{
			return new org.apache.hadoop.io.retry.RetryPolicies.ExponentialBackoffRetry(maxRetries
				, sleepTime, timeUnit);
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
		public static org.apache.hadoop.io.retry.RetryPolicy retryByException(org.apache.hadoop.io.retry.RetryPolicy
			 defaultPolicy, System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
			> exceptionToPolicyMap)
		{
			return new org.apache.hadoop.io.retry.RetryPolicies.ExceptionDependentRetry(defaultPolicy
				, exceptionToPolicyMap);
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
		public static org.apache.hadoop.io.retry.RetryPolicy retryByRemoteException(org.apache.hadoop.io.retry.RetryPolicy
			 defaultPolicy, System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
			> exceptionToPolicyMap)
		{
			return new org.apache.hadoop.io.retry.RetryPolicies.RemoteExceptionDependentRetry
				(defaultPolicy, exceptionToPolicyMap);
		}

		public static org.apache.hadoop.io.retry.RetryPolicy failoverOnNetworkException(int
			 maxFailovers)
		{
			return failoverOnNetworkException(TRY_ONCE_THEN_FAIL, maxFailovers);
		}

		public static org.apache.hadoop.io.retry.RetryPolicy failoverOnNetworkException(org.apache.hadoop.io.retry.RetryPolicy
			 fallbackPolicy, int maxFailovers)
		{
			return failoverOnNetworkException(fallbackPolicy, maxFailovers, 0, 0);
		}

		public static org.apache.hadoop.io.retry.RetryPolicy failoverOnNetworkException(org.apache.hadoop.io.retry.RetryPolicy
			 fallbackPolicy, int maxFailovers, long delayMillis, long maxDelayBase)
		{
			return new org.apache.hadoop.io.retry.RetryPolicies.FailoverOnNetworkExceptionRetry
				(fallbackPolicy, maxFailovers, delayMillis, maxDelayBase);
		}

		public static org.apache.hadoop.io.retry.RetryPolicy failoverOnNetworkException(org.apache.hadoop.io.retry.RetryPolicy
			 fallbackPolicy, int maxFailovers, int maxRetries, long delayMillis, long maxDelayBase
			)
		{
			return new org.apache.hadoop.io.retry.RetryPolicies.FailoverOnNetworkExceptionRetry
				(fallbackPolicy, maxFailovers, maxRetries, delayMillis, maxDelayBase);
		}

		internal class TryOnceThenFail : org.apache.hadoop.io.retry.RetryPolicy
		{
			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int retries, int failovers, bool isIdempotentOrAtMostOnce)
			{
				return org.apache.hadoop.io.retry.RetryPolicy.RetryAction.FAIL;
			}
		}

		internal class RetryForever : org.apache.hadoop.io.retry.RetryPolicy
		{
			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int retries, int failovers, bool isIdempotentOrAtMostOnce)
			{
				return org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RETRY;
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
		internal abstract class RetryLimited : org.apache.hadoop.io.retry.RetryPolicy
		{
			internal readonly int maxRetries;

			internal readonly long sleepTime;

			internal readonly java.util.concurrent.TimeUnit timeUnit;

			private string myString;

			internal RetryLimited(int maxRetries, long sleepTime, java.util.concurrent.TimeUnit
				 timeUnit)
			{
				if (maxRetries < 0)
				{
					throw new System.ArgumentException("maxRetries = " + maxRetries + " < 0");
				}
				if (sleepTime < 0)
				{
					throw new System.ArgumentException("sleepTime = " + sleepTime + " < 0");
				}
				this.maxRetries = maxRetries;
				this.sleepTime = sleepTime;
				this.timeUnit = timeUnit;
			}

			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int retries, int failovers, bool isIdempotentOrAtMostOnce)
			{
				if (retries >= maxRetries)
				{
					return org.apache.hadoop.io.retry.RetryPolicy.RetryAction.FAIL;
				}
				return new org.apache.hadoop.io.retry.RetryPolicy.RetryAction(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
					.RETRY, timeUnit.toMillis(calculateSleepTime(retries)));
			}

			protected internal abstract long calculateSleepTime(int retries);

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
					if (that == null || Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject
						(that))
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
					myString = Sharpen.Runtime.getClassForObject(this).getSimpleName() + "(maxRetries="
						 + maxRetries + ", sleepTime=" + sleepTime + " " + timeUnit + ")";
				}
				return myString;
			}
		}

		internal class RetryUpToMaximumCountWithFixedSleep : org.apache.hadoop.io.retry.RetryPolicies.RetryLimited
		{
			public RetryUpToMaximumCountWithFixedSleep(int maxRetries, long sleepTime, java.util.concurrent.TimeUnit
				 timeUnit)
				: base(maxRetries, sleepTime, timeUnit)
			{
			}

			protected internal override long calculateSleepTime(int retries)
			{
				return sleepTime;
			}
		}

		internal class RetryUpToMaximumTimeWithFixedSleep : org.apache.hadoop.io.retry.RetryPolicies.RetryUpToMaximumCountWithFixedSleep
		{
			public RetryUpToMaximumTimeWithFixedSleep(long maxTime, long sleepTime, java.util.concurrent.TimeUnit
				 timeUnit)
				: base((int)(maxTime / sleepTime), sleepTime, timeUnit)
			{
			}
		}

		internal class RetryUpToMaximumCountWithProportionalSleep : org.apache.hadoop.io.retry.RetryPolicies.RetryLimited
		{
			public RetryUpToMaximumCountWithProportionalSleep(int maxRetries, long sleepTime, 
				java.util.concurrent.TimeUnit timeUnit)
				: base(maxRetries, sleepTime, timeUnit)
			{
			}

			protected internal override long calculateSleepTime(int retries)
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
		public class MultipleLinearRandomRetry : org.apache.hadoop.io.retry.RetryPolicy
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
						throw new System.ArgumentException("numRetries = " + numRetries + " < 0");
					}
					if (sleepMillis < 0)
					{
						throw new System.ArgumentException("sleepMillis = " + sleepMillis + " < 0");
					}
					this.numRetries = numRetries;
					this.sleepMillis = sleepMillis;
				}

				public override string ToString()
				{
					return numRetries + "x" + sleepMillis + "ms";
				}
			}

			private readonly System.Collections.Generic.IList<org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair
				> pairs;

			private string myString;

			public MultipleLinearRandomRetry(System.Collections.Generic.IList<org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair
				> pairs)
			{
				if (pairs == null || pairs.isEmpty())
				{
					throw new System.ArgumentException("pairs must be neither null nor empty.");
				}
				this.pairs = java.util.Collections.unmodifiableList(pairs);
			}

			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int curRetry, int failovers, bool isIdempotentOrAtMostOnce)
			{
				org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair p = searchPair
					(curRetry);
				if (p == null)
				{
					//no more retries.
					return org.apache.hadoop.io.retry.RetryPolicy.RetryAction.FAIL;
				}
				//calculate sleep time and return.
				double ratio = RANDOM.get().nextDouble() + 0.5;
				//0.5 <= ratio <=1.5
				long sleepTime = System.Math.round(p.sleepMillis * ratio);
				return new org.apache.hadoop.io.retry.RetryPolicy.RetryAction(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
					.RETRY, sleepTime);
			}

			/// <summary>Given the current number of retry, search the corresponding pair.</summary>
			/// <returns>
			/// the corresponding pair,
			/// or null if the current number of retry &gt; maximum number of retry.
			/// </returns>
			private org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair searchPair
				(int curRetry)
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
					if (that == null || Sharpen.Runtime.getClassForObject(this) != Sharpen.Runtime.getClassForObject
						(that))
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
					myString = Sharpen.Runtime.getClassForObject(this).getSimpleName() + pairs;
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
			public static org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry 
				parseCommaSeparatedString(string s)
			{
				string[] elements = s.split(",");
				if (elements.Length == 0)
				{
					LOG.warn("Illegal value: there is no element in \"" + s + "\".");
					return null;
				}
				if (elements.Length % 2 != 0)
				{
					LOG.warn("Illegal value: the number of elements in \"" + s + "\" is " + elements.
						Length + " but an even number of elements is expected.");
					return null;
				}
				System.Collections.Generic.IList<org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair
					> pairs = new System.Collections.Generic.List<org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair
					>();
				for (int i = 0; i < elements.Length; )
				{
					//parse the i-th sleep-time
					int sleep = parsePositiveInt(elements, i++, s);
					if (sleep == -1)
					{
						return null;
					}
					//parse fails
					//parse the i-th number-of-retries
					int retries = parsePositiveInt(elements, i++, s);
					if (retries == -1)
					{
						return null;
					}
					//parse fails
					pairs.add(new org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry.Pair
						(retries, sleep));
				}
				return new org.apache.hadoop.io.retry.RetryPolicies.MultipleLinearRandomRetry(pairs
					);
			}

			/// <summary>Parse the i-th element as an integer.</summary>
			/// <returns>
			/// -1 if the parsing fails or the parsed value &lt;= 0;
			/// otherwise, return the parsed value.
			/// </returns>
			private static int parsePositiveInt(string[] elements, int i, string originalString
				)
			{
				string s = elements[i].Trim();
				int n;
				try
				{
					n = System.Convert.ToInt32(s);
				}
				catch (java.lang.NumberFormatException nfe)
				{
					LOG.warn("Failed to parse \"" + s + "\", which is the index " + i + " element in \""
						 + originalString + "\"", nfe);
					return -1;
				}
				if (n <= 0)
				{
					LOG.warn("The value " + n + " <= 0: it is parsed from the string \"" + s + "\" which is the index "
						 + i + " element in \"" + originalString + "\"");
					return -1;
				}
				return n;
			}
		}

		internal class ExceptionDependentRetry : org.apache.hadoop.io.retry.RetryPolicy
		{
			internal org.apache.hadoop.io.retry.RetryPolicy defaultPolicy;

			internal System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
				> exceptionToPolicyMap;

			public ExceptionDependentRetry(org.apache.hadoop.io.retry.RetryPolicy defaultPolicy
				, System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
				> exceptionToPolicyMap)
			{
				this.defaultPolicy = defaultPolicy;
				this.exceptionToPolicyMap = exceptionToPolicyMap;
			}

			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int retries, int failovers, bool isIdempotentOrAtMostOnce)
			{
				org.apache.hadoop.io.retry.RetryPolicy policy = exceptionToPolicyMap[Sharpen.Runtime.getClassForObject
					(e)];
				if (policy == null)
				{
					policy = defaultPolicy;
				}
				return policy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
			}
		}

		internal class RemoteExceptionDependentRetry : org.apache.hadoop.io.retry.RetryPolicy
		{
			internal org.apache.hadoop.io.retry.RetryPolicy defaultPolicy;

			internal System.Collections.Generic.IDictionary<string, org.apache.hadoop.io.retry.RetryPolicy
				> exceptionNameToPolicyMap;

			public RemoteExceptionDependentRetry(org.apache.hadoop.io.retry.RetryPolicy defaultPolicy
				, System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
				> exceptionToPolicyMap)
			{
				this.defaultPolicy = defaultPolicy;
				this.exceptionNameToPolicyMap = new System.Collections.Generic.Dictionary<string, 
					org.apache.hadoop.io.retry.RetryPolicy>();
				foreach (System.Collections.Generic.KeyValuePair<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
					> e in exceptionToPolicyMap)
				{
					exceptionNameToPolicyMap[e.Key.getName()] = e.Value;
				}
			}

			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int retries, int failovers, bool isIdempotentOrAtMostOnce)
			{
				org.apache.hadoop.io.retry.RetryPolicy policy = null;
				if (e is org.apache.hadoop.ipc.RemoteException)
				{
					policy = exceptionNameToPolicyMap[((org.apache.hadoop.ipc.RemoteException)e).getClassName
						()];
				}
				if (policy == null)
				{
					policy = defaultPolicy;
				}
				return policy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce);
			}
		}

		internal class ExponentialBackoffRetry : org.apache.hadoop.io.retry.RetryPolicies.RetryLimited
		{
			public ExponentialBackoffRetry(int maxRetries, long sleepTime, java.util.concurrent.TimeUnit
				 timeUnit)
				: base(maxRetries, sleepTime, timeUnit)
			{
				if (maxRetries < 0)
				{
					throw new System.ArgumentException("maxRetries = " + maxRetries + " < 0");
				}
				else
				{
					if (maxRetries >= long.SIZE - 1)
					{
						//calculateSleepTime may overflow. 
						throw new System.ArgumentException("maxRetries = " + maxRetries + " >= " + (long.
							SIZE - 1));
					}
				}
			}

			protected internal override long calculateSleepTime(int retries)
			{
				return calculateExponentialTime(sleepTime, retries + 1);
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
		internal class FailoverOnNetworkExceptionRetry : org.apache.hadoop.io.retry.RetryPolicy
		{
			private org.apache.hadoop.io.retry.RetryPolicy fallbackPolicy;

			private int maxFailovers;

			private int maxRetries;

			private long delayMillis;

			private long maxDelayBase;

			public FailoverOnNetworkExceptionRetry(org.apache.hadoop.io.retry.RetryPolicy fallbackPolicy
				, int maxFailovers)
				: this(fallbackPolicy, maxFailovers, 0, 0, 0)
			{
			}

			public FailoverOnNetworkExceptionRetry(org.apache.hadoop.io.retry.RetryPolicy fallbackPolicy
				, int maxFailovers, long delayMillis, long maxDelayBase)
				: this(fallbackPolicy, maxFailovers, 0, delayMillis, maxDelayBase)
			{
			}

			public FailoverOnNetworkExceptionRetry(org.apache.hadoop.io.retry.RetryPolicy fallbackPolicy
				, int maxFailovers, int maxRetries, long delayMillis, long maxDelayBase)
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
			private long getFailoverOrRetrySleepTime(int times)
			{
				return times == 0 ? 0 : calculateExponentialTime(delayMillis, times, maxDelayBase
					);
			}

			/// <exception cref="System.Exception"/>
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int retries, int failovers, bool isIdempotentOrAtMostOnce)
			{
				if (failovers >= maxFailovers)
				{
					return new org.apache.hadoop.io.retry.RetryPolicy.RetryAction(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
						.FAIL, 0, "failovers (" + failovers + ") exceeded maximum allowed (" + maxFailovers
						 + ")");
				}
				if (retries - failovers > maxRetries)
				{
					return new org.apache.hadoop.io.retry.RetryPolicy.RetryAction(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
						.FAIL, 0, "retries (" + retries + ") exceeded maximum allowed (" + maxRetries + 
						")");
				}
				if (e is java.net.ConnectException || e is java.net.NoRouteToHostException || e is
					 java.net.UnknownHostException || e is org.apache.hadoop.ipc.StandbyException ||
					 e is org.apache.hadoop.net.ConnectTimeoutException || isWrappedStandbyException
					(e))
				{
					return new org.apache.hadoop.io.retry.RetryPolicy.RetryAction(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
						.FAILOVER_AND_RETRY, getFailoverOrRetrySleepTime(failovers));
				}
				else
				{
					if (e is org.apache.hadoop.ipc.RetriableException || getWrappedRetriableException
						(e) != null)
					{
						// RetriableException or RetriableException wrapped 
						return new org.apache.hadoop.io.retry.RetryPolicy.RetryAction(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
							.RETRY, getFailoverOrRetrySleepTime(retries));
					}
					else
					{
						if (e is System.Net.Sockets.SocketException || (e is System.IO.IOException && !(e
							 is org.apache.hadoop.ipc.RemoteException)))
						{
							if (isIdempotentOrAtMostOnce)
							{
								return org.apache.hadoop.io.retry.RetryPolicy.RetryAction.FAILOVER_AND_RETRY;
							}
							else
							{
								return new org.apache.hadoop.io.retry.RetryPolicy.RetryAction(org.apache.hadoop.io.retry.RetryPolicy.RetryAction.RetryDecision
									.FAIL, 0, "the invoked method is not idempotent, and unable to determine " + "whether it was invoked"
									);
							}
						}
						else
						{
							return fallbackPolicy.shouldRetry(e, retries, failovers, isIdempotentOrAtMostOnce
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
		private static long calculateExponentialTime(long time, int retries, long cap)
		{
			long baseTime = System.Math.min(time * (1L << retries), cap);
			return (long)(baseTime * (RANDOM.get().nextDouble() + 0.5));
		}

		private static long calculateExponentialTime(long time, int retries)
		{
			return calculateExponentialTime(time, retries, long.MaxValue);
		}

		private static bool isWrappedStandbyException(System.Exception e)
		{
			if (!(e is org.apache.hadoop.ipc.RemoteException))
			{
				return false;
			}
			System.Exception unwrapped = ((org.apache.hadoop.ipc.RemoteException)e).unwrapRemoteException
				(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.StandbyException))
				);
			return unwrapped is org.apache.hadoop.ipc.StandbyException;
		}

		internal static org.apache.hadoop.ipc.RetriableException getWrappedRetriableException
			(System.Exception e)
		{
			if (!(e is org.apache.hadoop.ipc.RemoteException))
			{
				return null;
			}
			System.Exception unwrapped = ((org.apache.hadoop.ipc.RemoteException)e).unwrapRemoteException
				(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RetriableException
				)));
			return unwrapped is org.apache.hadoop.ipc.RetriableException ? (org.apache.hadoop.ipc.RetriableException
				)unwrapped : null;
		}
	}
}
