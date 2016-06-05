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
using System.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.IO.Retry
{
	public class TestFailoverProxy
	{
		public class FlipFlopProxyProvider<T> : FailoverProxyProvider<T>
		{
			private Type iface;

			private T currentlyActive;

			private T impl1;

			private T impl2;

			private int failoversOccurred = 0;

			public FlipFlopProxyProvider(Type iface, T activeImpl, T standbyImpl)
			{
				this.iface = iface;
				this.impl1 = activeImpl;
				this.impl2 = standbyImpl;
				currentlyActive = impl1;
			}

			public override FailoverProxyProvider.ProxyInfo<T> GetProxy()
			{
				return new FailoverProxyProvider.ProxyInfo<T>(currentlyActive, currentlyActive.ToString
					());
			}

			public override void PerformFailover(object currentProxy)
			{
				lock (this)
				{
					currentlyActive = impl1 == currentProxy ? impl2 : impl1;
					failoversOccurred++;
				}
			}

			public override Type GetInterface()
			{
				return iface;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			// Nothing to do.
			public virtual int GetFailoversOccurred()
			{
				return failoversOccurred;
			}
		}

		public class FailOverOnceOnAnyExceptionPolicy : RetryPolicy
		{
			public override RetryPolicy.RetryAction ShouldRetry(Exception e, int retries, int
				 failovers, bool isIdempotentOrAtMostOnce)
			{
				return failovers < 1 ? RetryPolicy.RetryAction.FailoverAndRetry : RetryPolicy.RetryAction
					.Fail;
			}
		}

		private static TestFailoverProxy.FlipFlopProxyProvider<UnreliableInterface> NewFlipFlopProxyProvider
			()
		{
			return new TestFailoverProxy.FlipFlopProxyProvider<UnreliableInterface>(typeof(UnreliableInterface
				), new UnreliableImplementation("impl1"), new UnreliableImplementation("impl2"));
		}

		private static TestFailoverProxy.FlipFlopProxyProvider<UnreliableInterface> NewFlipFlopProxyProvider
			(UnreliableImplementation.TypeOfExceptionToFailWith t1, UnreliableImplementation.TypeOfExceptionToFailWith
			 t2)
		{
			return new TestFailoverProxy.FlipFlopProxyProvider<UnreliableInterface>(typeof(UnreliableInterface
				), new UnreliableImplementation("impl1", t1), new UnreliableImplementation("impl2"
				, t2));
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		[Fact]
		public virtual void TestSuccedsOnceThenFailOver()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(NewFlipFlopProxyProvider(), new TestFailoverProxy.FailOverOnceOnAnyExceptionPolicy
				());
			Assert.Equal("impl1", unreliable.SucceedsOnceThenFailsReturningString
				());
			Assert.Equal("impl2", unreliable.SucceedsOnceThenFailsReturningString
				());
			try
			{
				unreliable.SucceedsOnceThenFailsReturningString();
				NUnit.Framework.Assert.Fail("should not have succeeded more than twice");
			}
			catch (UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		[Fact]
		public virtual void TestSucceedsTenTimesThenFailOver()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(NewFlipFlopProxyProvider(), new TestFailoverProxy.FailOverOnceOnAnyExceptionPolicy
				());
			for (int i = 0; i < 10; i++)
			{
				Assert.Equal("impl1", unreliable.SucceedsTenTimesThenFailsReturningString
					());
			}
			Assert.Equal("impl2", unreliable.SucceedsTenTimesThenFailsReturningString
				());
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		[Fact]
		public virtual void TestNeverFailOver()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(NewFlipFlopProxyProvider(), RetryPolicies.TryOnceThenFail);
			unreliable.SucceedsOnceThenFailsReturningString();
			try
			{
				unreliable.SucceedsOnceThenFailsReturningString();
				NUnit.Framework.Assert.Fail("should not have succeeded twice");
			}
			catch (UnreliableInterface.UnreliableException e)
			{
				Assert.Equal("impl1", e.Message);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		[Fact]
		public virtual void TestFailoverOnStandbyException()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(NewFlipFlopProxyProvider(), RetryPolicies.FailoverOnNetworkException(1));
			Assert.Equal("impl1", unreliable.SucceedsOnceThenFailsReturningString
				());
			try
			{
				unreliable.SucceedsOnceThenFailsReturningString();
				NUnit.Framework.Assert.Fail("should not have succeeded twice");
			}
			catch (UnreliableInterface.UnreliableException e)
			{
				// Make sure there was no failover on normal exception.
				Assert.Equal("impl1", e.Message);
			}
			unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface>(NewFlipFlopProxyProvider
				(UnreliableImplementation.TypeOfExceptionToFailWith.StandbyException, UnreliableImplementation.TypeOfExceptionToFailWith
				.UnreliableException), RetryPolicies.FailoverOnNetworkException(1));
			Assert.Equal("impl1", unreliable.SucceedsOnceThenFailsReturningString
				());
			// Make sure we fail over since the first implementation threw a StandbyException
			Assert.Equal("impl2", unreliable.SucceedsOnceThenFailsReturningString
				());
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		[Fact]
		public virtual void TestFailoverOnNetworkExceptionIdempotentOperation()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(NewFlipFlopProxyProvider(UnreliableImplementation.TypeOfExceptionToFailWith.IoException
				, UnreliableImplementation.TypeOfExceptionToFailWith.UnreliableException), RetryPolicies
				.FailoverOnNetworkException(1));
			Assert.Equal("impl1", unreliable.SucceedsOnceThenFailsReturningString
				());
			try
			{
				unreliable.SucceedsOnceThenFailsReturningString();
				NUnit.Framework.Assert.Fail("should not have succeeded twice");
			}
			catch (IOException e)
			{
				// Make sure we *don't* fail over since the first implementation threw an
				// IOException and this method is not idempotent
				Assert.Equal("impl1", e.Message);
			}
			Assert.Equal("impl1", unreliable.SucceedsOnceThenFailsReturningStringIdempotent
				());
			// Make sure we fail over since the first implementation threw an
			// IOException and this method is idempotent.
			Assert.Equal("impl2", unreliable.SucceedsOnceThenFailsReturningStringIdempotent
				());
		}

		/// <summary>
		/// Test that if a non-idempotent void function is called, and there is an exception,
		/// the exception is properly propagated
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestExceptionPropagatedForNonIdempotentVoid()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(NewFlipFlopProxyProvider(UnreliableImplementation.TypeOfExceptionToFailWith.IoException
				, UnreliableImplementation.TypeOfExceptionToFailWith.UnreliableException), RetryPolicies
				.FailoverOnNetworkException(1));
			try
			{
				unreliable.NonIdempotentVoidFailsIfIdentifierDoesntMatch("impl2");
				NUnit.Framework.Assert.Fail("did not throw an exception");
			}
			catch (Exception)
			{
			}
		}

		private class SynchronizedUnreliableImplementation : UnreliableImplementation
		{
			private CountDownLatch methodLatch;

			public SynchronizedUnreliableImplementation(string identifier, UnreliableImplementation.TypeOfExceptionToFailWith
				 exceptionToFailWith, int threadCount)
				: base(identifier, exceptionToFailWith)
			{
				methodLatch = new CountDownLatch(threadCount);
			}

			/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
			/// 	"/>
			/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
			/// <exception cref="System.IO.IOException"/>
			public override string FailsIfIdentifierDoesntMatch(string identifier)
			{
				// Wait until all threads are trying to invoke this method
				methodLatch.CountDown();
				try
				{
					methodLatch.Await();
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
				return base.FailsIfIdentifierDoesntMatch(identifier);
			}
		}

		private class ConcurrentMethodThread : Sharpen.Thread
		{
			private UnreliableInterface unreliable;

			public string result;

			public ConcurrentMethodThread(UnreliableInterface unreliable)
			{
				this.unreliable = unreliable;
			}

			public override void Run()
			{
				try
				{
					result = unreliable.FailsIfIdentifierDoesntMatch("impl2");
				}
				catch (Exception e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		/// <summary>
		/// Test that concurrent failed method invocations only result in a single
		/// failover.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConcurrentMethodFailures()
		{
			TestFailoverProxy.FlipFlopProxyProvider<UnreliableInterface> proxyProvider = new 
				TestFailoverProxy.FlipFlopProxyProvider<UnreliableInterface>(typeof(UnreliableInterface
				), new TestFailoverProxy.SynchronizedUnreliableImplementation("impl1", UnreliableImplementation.TypeOfExceptionToFailWith
				.StandbyException, 2), new UnreliableImplementation("impl2", UnreliableImplementation.TypeOfExceptionToFailWith
				.StandbyException));
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(proxyProvider, RetryPolicies.FailoverOnNetworkException(10));
			TestFailoverProxy.ConcurrentMethodThread t1 = new TestFailoverProxy.ConcurrentMethodThread
				(unreliable);
			TestFailoverProxy.ConcurrentMethodThread t2 = new TestFailoverProxy.ConcurrentMethodThread
				(unreliable);
			t1.Start();
			t2.Start();
			t1.Join();
			t2.Join();
			Assert.Equal("impl2", t1.result);
			Assert.Equal("impl2", t2.result);
			Assert.Equal(1, proxyProvider.GetFailoversOccurred());
		}

		/// <summary>
		/// Ensure that when all configured services are throwing StandbyException
		/// that we fail over back and forth between them until one is no longer
		/// throwing StandbyException.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestFailoverBetweenMultipleStandbys()
		{
			long millisToSleep = 10000;
			UnreliableImplementation impl1 = new UnreliableImplementation("impl1", UnreliableImplementation.TypeOfExceptionToFailWith
				.StandbyException);
			TestFailoverProxy.FlipFlopProxyProvider<UnreliableInterface> proxyProvider = new 
				TestFailoverProxy.FlipFlopProxyProvider<UnreliableInterface>(typeof(UnreliableInterface
				), impl1, new UnreliableImplementation("impl2", UnreliableImplementation.TypeOfExceptionToFailWith
				.StandbyException));
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(proxyProvider, RetryPolicies.FailoverOnNetworkException(RetryPolicies.TryOnceThenFail
				, 10, 1000, 10000));
			new _Thread_328(millisToSleep, impl1).Start();
			string result = unreliable.FailsIfIdentifierDoesntMatch("renamed-impl1");
			Assert.Equal("renamed-impl1", result);
		}

		private sealed class _Thread_328 : Sharpen.Thread
		{
			public _Thread_328(long millisToSleep, UnreliableImplementation impl1)
			{
				this.millisToSleep = millisToSleep;
				this.impl1 = impl1;
			}

			public override void Run()
			{
				ThreadUtil.SleepAtLeastIgnoreInterrupts(millisToSleep);
				impl1.SetIdentifier("renamed-impl1");
			}

			private readonly long millisToSleep;

			private readonly UnreliableImplementation impl1;
		}

		/// <summary>Ensure that normal IO exceptions don't result in a failover.</summary>
		[Fact]
		public virtual void TestExpectedIOException()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(NewFlipFlopProxyProvider(UnreliableImplementation.TypeOfExceptionToFailWith.RemoteException
				, UnreliableImplementation.TypeOfExceptionToFailWith.UnreliableException), RetryPolicies
				.FailoverOnNetworkException(RetryPolicies.TryOnceThenFail, 10, 1000, 10000));
			try
			{
				unreliable.FailsIfIdentifierDoesntMatch("no-such-identifier");
				NUnit.Framework.Assert.Fail("Should have thrown *some* exception");
			}
			catch (Exception e)
			{
				Assert.True("Expected IOE but got " + e.GetType(), e is IOException
					);
			}
		}
	}
}
