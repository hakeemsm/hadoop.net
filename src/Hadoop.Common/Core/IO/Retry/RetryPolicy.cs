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


namespace Org.Apache.Hadoop.IO.Retry
{
	/// <summary>
	/// <p>
	/// Specifies a policy for retrying method failures.
	/// </summary>
	/// <remarks>
	/// <p>
	/// Specifies a policy for retrying method failures.
	/// Implementations of this interface should be immutable.
	/// </p>
	/// </remarks>
	public abstract class RetryPolicy
	{
		/// <summary>
		/// Returned by
		/// <see cref="RetryPolicy.ShouldRetry(System.Exception, int, int, bool)"/>
		/// .
		/// </summary>
		public class RetryAction
		{
			public static readonly RetryPolicy.RetryAction Fail = new RetryPolicy.RetryAction
				(RetryPolicy.RetryAction.RetryDecision.Fail);

			public static readonly RetryPolicy.RetryAction Retry = new RetryPolicy.RetryAction
				(RetryPolicy.RetryAction.RetryDecision.Retry);

			public static readonly RetryPolicy.RetryAction FailoverAndRetry = new RetryPolicy.RetryAction
				(RetryPolicy.RetryAction.RetryDecision.FailoverAndRetry);

			public readonly RetryPolicy.RetryAction.RetryDecision action;

			public readonly long delayMillis;

			public readonly string reason;

			public RetryAction(RetryPolicy.RetryAction.RetryDecision action)
				: this(action, 0, null)
			{
			}

			public RetryAction(RetryPolicy.RetryAction.RetryDecision action, long delayTime)
				: this(action, delayTime, null)
			{
			}

			public RetryAction(RetryPolicy.RetryAction.RetryDecision action, long delayTime, 
				string reason)
			{
				// A few common retry policies, with no delays.
				this.action = action;
				this.delayMillis = delayTime;
				this.reason = reason;
			}

			public override string ToString()
			{
				return GetType().Name + "(action=" + action + ", delayMillis=" + delayMillis + ", reason="
					 + reason + ")";
			}

			public enum RetryDecision
			{
				Fail,
				Retry,
				FailoverAndRetry
			}
		}

		/// <summary>
		/// <p>
		/// Determines whether the framework should retry a method for the given
		/// exception, and the number of retries that have been made for that operation
		/// so far.
		/// </summary>
		/// <remarks>
		/// <p>
		/// Determines whether the framework should retry a method for the given
		/// exception, and the number of retries that have been made for that operation
		/// so far.
		/// </p>
		/// </remarks>
		/// <param name="e">The exception that caused the method to fail</param>
		/// <param name="retries">The number of times the method has been retried</param>
		/// <param name="failovers">
		/// The number of times the method has failed over to a
		/// different backend implementation
		/// </param>
		/// <param name="isIdempotentOrAtMostOnce">
		/// <code>true</code> if the method is
		/// <see cref="Idempotent"/>
		/// or
		/// <see cref="AtMostOnce"/>
		/// and so can reasonably be
		/// retried on failover when we don't know if the previous attempt
		/// reached the server or not
		/// </param>
		/// <returns>
		/// <code>true</code> if the method should be retried,
		/// <code>false</code> if the method should not be retried but
		/// shouldn't fail with an exception (only for void methods)
		/// </returns>
		/// <exception cref="System.Exception">
		/// The re-thrown exception <code>e</code> indicating that
		/// the method failed and should not be retried further
		/// </exception>
		public abstract RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
			 failovers, bool isIdempotentOrAtMostOnce);
	}

	public static class RetryPolicyConstants
	{
	}
}
