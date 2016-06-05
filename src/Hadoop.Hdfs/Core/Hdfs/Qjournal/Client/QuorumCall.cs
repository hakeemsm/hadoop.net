using System;
using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using Com.Google.Protobuf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Client
{
	/// <summary>Represents a set of calls for which a quorum of results is needed.</summary>
	/// <?/>
	/// <?/>
	internal class QuorumCall<Key, Result>
	{
		private readonly IDictionary<KEY, RESULT> successes = Maps.NewHashMap();

		private readonly IDictionary<KEY, Exception> exceptions = Maps.NewHashMap();

		/// <summary>
		/// Interval, in milliseconds, at which a log message will be made
		/// while waiting for a quorum call.
		/// </summary>
		private const int WaitProgressIntervalMillis = 1000;

		/// <summary>
		/// Start logging messages at INFO level periodically after waiting for
		/// this fraction of the configured timeout for any call.
		/// </summary>
		private const float WaitProgressInfoThreshold = 0.3f;

		/// <summary>
		/// Start logging messages at WARN level after waiting for this
		/// fraction of the configured timeout for any call.
		/// </summary>
		private const float WaitProgressWarnThreshold = 0.7f;

		internal static Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumCall<KEY, RESULT> Create
			<Key, Result>(IDictionary<KEY, ListenableFuture<RESULT>> calls)
		{
			Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumCall<KEY, RESULT> qr = new Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumCall
				<KEY, RESULT>();
			foreach (KeyValuePair<KEY, ListenableFuture<RESULT>> e in calls)
			{
				Preconditions.CheckArgument(e.Value != null, "null future for key: " + e.Key);
				Futures.AddCallback(e.Value, new _FutureCallback_68(qr, e));
			}
			return qr;
		}

		private sealed class _FutureCallback_68 : FutureCallback<RESULT>
		{
			public _FutureCallback_68(Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumCall<KEY, 
				RESULT> qr, KeyValuePair<KEY, ListenableFuture<RESULT>> e)
			{
				this.qr = qr;
				this.e = e;
			}

			public void OnFailure(Exception t)
			{
				qr.AddException(e.Key, t);
			}

			public void OnSuccess(RESULT res)
			{
				qr.AddResult(e.Key, res);
			}

			private readonly Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumCall<KEY, RESULT> qr;

			private readonly KeyValuePair<KEY, ListenableFuture<RESULT>> e;
		}

		private QuorumCall()
		{
		}

		// Only instantiated from factory method above
		/// <summary>Wait for the quorum to achieve a certain number of responses.</summary>
		/// <remarks>
		/// Wait for the quorum to achieve a certain number of responses.
		/// Note that, even after this returns, more responses may arrive,
		/// causing the return value of other methods in this class to change.
		/// </remarks>
		/// <param name="minResponses">
		/// return as soon as this many responses have been
		/// received, regardless of whether they are successes or exceptions
		/// </param>
		/// <param name="minSuccesses">
		/// return as soon as this many successful (non-exception)
		/// responses have been received
		/// </param>
		/// <param name="maxExceptions">
		/// return as soon as this many exception responses
		/// have been received. Pass 0 to return immediately if any exception is
		/// received.
		/// </param>
		/// <param name="millis">the number of milliseconds to wait for</param>
		/// <exception cref="System.Exception">if the thread is interrupted while waiting</exception>
		/// <exception cref="Sharpen.TimeoutException">
		/// if the specified timeout elapses before
		/// achieving the desired conditions
		/// </exception>
		public virtual void WaitFor(int minResponses, int minSuccesses, int maxExceptions
			, int millis, string operationName)
		{
			lock (this)
			{
				long st = Time.MonotonicNow();
				long nextLogTime = st + (long)(millis * WaitProgressInfoThreshold);
				long et = st + millis;
				while (true)
				{
					CheckAssertionErrors();
					if (minResponses > 0 && CountResponses() >= minResponses)
					{
						return;
					}
					if (minSuccesses > 0 && CountSuccesses() >= minSuccesses)
					{
						return;
					}
					if (maxExceptions >= 0 && CountExceptions() > maxExceptions)
					{
						return;
					}
					long now = Time.MonotonicNow();
					if (now > nextLogTime)
					{
						long waited = now - st;
						string msg = string.Format("Waited %s ms (timeout=%s ms) for a response for %s", 
							waited, millis, operationName);
						if (!successes.IsEmpty())
						{
							msg += ". Succeeded so far: [" + Joiner.On(",").Join(successes.Keys) + "]";
						}
						if (!exceptions.IsEmpty())
						{
							msg += ". Exceptions so far: [" + GetExceptionMapString() + "]";
						}
						if (successes.IsEmpty() && exceptions.IsEmpty())
						{
							msg += ". No responses yet.";
						}
						if (waited > millis * WaitProgressWarnThreshold)
						{
							QuorumJournalManager.Log.Warn(msg);
						}
						else
						{
							QuorumJournalManager.Log.Info(msg);
						}
						nextLogTime = now + WaitProgressIntervalMillis;
					}
					long rem = et - now;
					if (rem <= 0)
					{
						throw new TimeoutException();
					}
					rem = Math.Min(rem, nextLogTime - now);
					rem = Math.Max(rem, 1);
					Sharpen.Runtime.Wait(this, rem);
				}
			}
		}

		/// <summary>Check if any of the responses came back with an AssertionError.</summary>
		/// <remarks>
		/// Check if any of the responses came back with an AssertionError.
		/// If so, it re-throws it, even if there was a quorum of responses.
		/// This code only runs if assertions are enabled for this class,
		/// otherwise it should JIT itself away.
		/// This is done since AssertionError indicates programmer confusion
		/// rather than some kind of expected issue, and thus in the context
		/// of test cases we'd like to actually fail the test case instead of
		/// continuing through.
		/// </remarks>
		private void CheckAssertionErrors()
		{
			lock (this)
			{
				bool assertsEnabled = false;
				System.Diagnostics.Debug.Assert(assertsEnabled = true);
				// sets to true if enabled
				if (assertsEnabled)
				{
					foreach (Exception t in exceptions.Values)
					{
						if (t is Exception)
						{
							throw (Exception)t;
						}
						else
						{
							if (t is RemoteException && ((RemoteException)t).GetClassName().Equals(typeof(Exception
								).FullName))
							{
								throw new Exception(t);
							}
						}
					}
				}
			}
		}

		private void AddResult(KEY k, RESULT res)
		{
			lock (this)
			{
				successes[k] = res;
				Sharpen.Runtime.NotifyAll(this);
			}
		}

		private void AddException(KEY k, Exception t)
		{
			lock (this)
			{
				exceptions[k] = t;
				Sharpen.Runtime.NotifyAll(this);
			}
		}

		/// <returns>
		/// the total number of calls for which a response has been received,
		/// regardless of whether it threw an exception or returned a successful
		/// result.
		/// </returns>
		public virtual int CountResponses()
		{
			lock (this)
			{
				return successes.Count + exceptions.Count;
			}
		}

		/// <returns>
		/// the number of calls for which a non-exception response has been
		/// received.
		/// </returns>
		public virtual int CountSuccesses()
		{
			lock (this)
			{
				return successes.Count;
			}
		}

		/// <returns>
		/// the number of calls for which an exception response has been
		/// received.
		/// </returns>
		public virtual int CountExceptions()
		{
			lock (this)
			{
				return exceptions.Count;
			}
		}

		/// <returns>
		/// the map of successful responses. A copy is made such that this
		/// map will not be further mutated, even if further results arrive for the
		/// quorum.
		/// </returns>
		public virtual IDictionary<KEY, RESULT> GetResults()
		{
			lock (this)
			{
				return Maps.NewHashMap(successes);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Qjournal.Client.QuorumException"/>
		public virtual void RethrowException(string msg)
		{
			lock (this)
			{
				Preconditions.CheckState(!exceptions.IsEmpty());
				throw QuorumException.Create(msg, successes, exceptions);
			}
		}

		public static string MapToString<K>(IDictionary<K, Message> map)
		{
			StringBuilder sb = new StringBuilder();
			bool first = true;
			foreach (KeyValuePair<K, Message> e in map)
			{
				if (!first)
				{
					sb.Append("\n");
				}
				first = false;
				sb.Append(e.Key).Append(": ").Append(TextFormat.ShortDebugString(e.Value));
			}
			return sb.ToString();
		}

		/// <summary>
		/// Return a string suitable for displaying to the user, containing
		/// any exceptions that have been received so far.
		/// </summary>
		private string GetExceptionMapString()
		{
			StringBuilder sb = new StringBuilder();
			bool first = true;
			foreach (KeyValuePair<KEY, Exception> e in exceptions)
			{
				if (!first)
				{
					sb.Append(", ");
				}
				first = false;
				sb.Append(e.Key).Append(": ").Append(e.Value.GetLocalizedMessage());
			}
			return sb.ToString();
		}
	}
}
