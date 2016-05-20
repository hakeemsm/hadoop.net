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
	public class TestFailoverProxy
	{
		public class FlipFlopProxyProvider<T> : org.apache.hadoop.io.retry.FailoverProxyProvider
			<T>
		{
			private java.lang.Class iface;

			private T currentlyActive;

			private T impl1;

			private T impl2;

			private int failoversOccurred = 0;

			public FlipFlopProxyProvider(java.lang.Class iface, T activeImpl, T standbyImpl)
			{
				this.iface = iface;
				this.impl1 = activeImpl;
				this.impl2 = standbyImpl;
				currentlyActive = impl1;
			}

			public override org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo<T> getProxy
				()
			{
				return new org.apache.hadoop.io.retry.FailoverProxyProvider.ProxyInfo<T>(currentlyActive
					, currentlyActive.ToString());
			}

			public override void performFailover(object currentProxy)
			{
				lock (this)
				{
					currentlyActive = impl1 == currentProxy ? impl2 : impl1;
					failoversOccurred++;
				}
			}

			public override java.lang.Class getInterface()
			{
				return iface;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
			}

			// Nothing to do.
			public virtual int getFailoversOccurred()
			{
				return failoversOccurred;
			}
		}

		public class FailOverOnceOnAnyExceptionPolicy : org.apache.hadoop.io.retry.RetryPolicy
		{
			public override org.apache.hadoop.io.retry.RetryPolicy.RetryAction shouldRetry(System.Exception
				 e, int retries, int failovers, bool isIdempotentOrAtMostOnce)
			{
				return failovers < 1 ? org.apache.hadoop.io.retry.RetryPolicy.RetryAction.FAILOVER_AND_RETRY
					 : org.apache.hadoop.io.retry.RetryPolicy.RetryAction.FAIL;
			}
		}

		private static org.apache.hadoop.io.retry.TestFailoverProxy.FlipFlopProxyProvider
			<org.apache.hadoop.io.retry.UnreliableInterface> newFlipFlopProxyProvider()
		{
			return new org.apache.hadoop.io.retry.TestFailoverProxy.FlipFlopProxyProvider<org.apache.hadoop.io.retry.UnreliableInterface
				>(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.retry.UnreliableInterface
				)), new org.apache.hadoop.io.retry.UnreliableImplementation("impl1"), new org.apache.hadoop.io.retry.UnreliableImplementation
				("impl2"));
		}

		private static org.apache.hadoop.io.retry.TestFailoverProxy.FlipFlopProxyProvider
			<org.apache.hadoop.io.retry.UnreliableInterface> newFlipFlopProxyProvider(org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
			 t1, org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
			 t2)
		{
			return new org.apache.hadoop.io.retry.TestFailoverProxy.FlipFlopProxyProvider<org.apache.hadoop.io.retry.UnreliableInterface
				>(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.retry.UnreliableInterface
				)), new org.apache.hadoop.io.retry.UnreliableImplementation("impl1", t1), new org.apache.hadoop.io.retry.UnreliableImplementation
				("impl2", t2));
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		[NUnit.Framework.Test]
		public virtual void testSuccedsOnceThenFailOver()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(newFlipFlopProxyProvider(), new org.apache.hadoop.io.retry.TestFailoverProxy.FailOverOnceOnAnyExceptionPolicy
				());
			NUnit.Framework.Assert.AreEqual("impl1", unreliable.succeedsOnceThenFailsReturningString
				());
			NUnit.Framework.Assert.AreEqual("impl2", unreliable.succeedsOnceThenFailsReturningString
				());
			try
			{
				unreliable.succeedsOnceThenFailsReturningString();
				NUnit.Framework.Assert.Fail("should not have succeeded more than twice");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		[NUnit.Framework.Test]
		public virtual void testSucceedsTenTimesThenFailOver()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(newFlipFlopProxyProvider(), new org.apache.hadoop.io.retry.TestFailoverProxy.FailOverOnceOnAnyExceptionPolicy
				());
			for (int i = 0; i < 10; i++)
			{
				NUnit.Framework.Assert.AreEqual("impl1", unreliable.succeedsTenTimesThenFailsReturningString
					());
			}
			NUnit.Framework.Assert.AreEqual("impl2", unreliable.succeedsTenTimesThenFailsReturningString
				());
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		[NUnit.Framework.Test]
		public virtual void testNeverFailOver()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(newFlipFlopProxyProvider(), org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL
				);
			unreliable.succeedsOnceThenFailsReturningString();
			try
			{
				unreliable.succeedsOnceThenFailsReturningString();
				NUnit.Framework.Assert.Fail("should not have succeeded twice");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException e)
			{
				NUnit.Framework.Assert.AreEqual("impl1", e.Message);
			}
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverOnStandbyException()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(newFlipFlopProxyProvider(), org.apache.hadoop.io.retry.RetryPolicies.failoverOnNetworkException
				(1));
			NUnit.Framework.Assert.AreEqual("impl1", unreliable.succeedsOnceThenFailsReturningString
				());
			try
			{
				unreliable.succeedsOnceThenFailsReturningString();
				NUnit.Framework.Assert.Fail("should not have succeeded twice");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException e)
			{
				// Make sure there was no failover on normal exception.
				NUnit.Framework.Assert.AreEqual("impl1", e.Message);
			}
			unreliable = (org.apache.hadoop.io.retry.UnreliableInterface)org.apache.hadoop.io.retry.RetryProxy
				.create<org.apache.hadoop.io.retry.UnreliableInterface>(newFlipFlopProxyProvider
				(org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith.STANDBY_EXCEPTION
				, org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith.
				UNRELIABLE_EXCEPTION), org.apache.hadoop.io.retry.RetryPolicies.failoverOnNetworkException
				(1));
			NUnit.Framework.Assert.AreEqual("impl1", unreliable.succeedsOnceThenFailsReturningString
				());
			// Make sure we fail over since the first implementation threw a StandbyException
			NUnit.Framework.Assert.AreEqual("impl2", unreliable.succeedsOnceThenFailsReturningString
				());
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverOnNetworkExceptionIdempotentOperation()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(newFlipFlopProxyProvider(org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.IO_EXCEPTION, org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.UNRELIABLE_EXCEPTION), org.apache.hadoop.io.retry.RetryPolicies.failoverOnNetworkException
				(1));
			NUnit.Framework.Assert.AreEqual("impl1", unreliable.succeedsOnceThenFailsReturningString
				());
			try
			{
				unreliable.succeedsOnceThenFailsReturningString();
				NUnit.Framework.Assert.Fail("should not have succeeded twice");
			}
			catch (System.IO.IOException e)
			{
				// Make sure we *don't* fail over since the first implementation threw an
				// IOException and this method is not idempotent
				NUnit.Framework.Assert.AreEqual("impl1", e.Message);
			}
			NUnit.Framework.Assert.AreEqual("impl1", unreliable.succeedsOnceThenFailsReturningStringIdempotent
				());
			// Make sure we fail over since the first implementation threw an
			// IOException and this method is idempotent.
			NUnit.Framework.Assert.AreEqual("impl2", unreliable.succeedsOnceThenFailsReturningStringIdempotent
				());
		}

		/// <summary>
		/// Test that if a non-idempotent void function is called, and there is an exception,
		/// the exception is properly propagated
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testExceptionPropagatedForNonIdempotentVoid()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(newFlipFlopProxyProvider(org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.IO_EXCEPTION, org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.UNRELIABLE_EXCEPTION), org.apache.hadoop.io.retry.RetryPolicies.failoverOnNetworkException
				(1));
			try
			{
				unreliable.nonIdempotentVoidFailsIfIdentifierDoesntMatch("impl2");
				NUnit.Framework.Assert.Fail("did not throw an exception");
			}
			catch (System.Exception)
			{
			}
		}

		private class SynchronizedUnreliableImplementation : org.apache.hadoop.io.retry.UnreliableImplementation
		{
			private java.util.concurrent.CountDownLatch methodLatch;

			public SynchronizedUnreliableImplementation(string identifier, org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				 exceptionToFailWith, int threadCount)
				: base(identifier, exceptionToFailWith)
			{
				methodLatch = new java.util.concurrent.CountDownLatch(threadCount);
			}

			/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
			/// 	"/>
			/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
			/// <exception cref="System.IO.IOException"/>
			public override string failsIfIdentifierDoesntMatch(string identifier)
			{
				// Wait until all threads are trying to invoke this method
				methodLatch.countDown();
				try
				{
					methodLatch.await();
				}
				catch (System.Exception e)
				{
					throw new System.Exception(e);
				}
				return base.failsIfIdentifierDoesntMatch(identifier);
			}
		}

		private class ConcurrentMethodThread : java.lang.Thread
		{
			private org.apache.hadoop.io.retry.UnreliableInterface unreliable;

			public string result;

			public ConcurrentMethodThread(org.apache.hadoop.io.retry.UnreliableInterface unreliable
				)
			{
				this.unreliable = unreliable;
			}

			public override void run()
			{
				try
				{
					result = unreliable.failsIfIdentifierDoesntMatch("impl2");
				}
				catch (System.Exception e)
				{
					throw new System.Exception(e);
				}
			}
		}

		/// <summary>
		/// Test that concurrent failed method invocations only result in a single
		/// failover.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConcurrentMethodFailures()
		{
			org.apache.hadoop.io.retry.TestFailoverProxy.FlipFlopProxyProvider<org.apache.hadoop.io.retry.UnreliableInterface
				> proxyProvider = new org.apache.hadoop.io.retry.TestFailoverProxy.FlipFlopProxyProvider
				<org.apache.hadoop.io.retry.UnreliableInterface>(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.retry.UnreliableInterface)), new org.apache.hadoop.io.retry.TestFailoverProxy.SynchronizedUnreliableImplementation
				("impl1", org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.STANDBY_EXCEPTION, 2), new org.apache.hadoop.io.retry.UnreliableImplementation(
				"impl2", org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.STANDBY_EXCEPTION));
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(proxyProvider, org.apache.hadoop.io.retry.RetryPolicies.failoverOnNetworkException
				(10));
			org.apache.hadoop.io.retry.TestFailoverProxy.ConcurrentMethodThread t1 = new org.apache.hadoop.io.retry.TestFailoverProxy.ConcurrentMethodThread
				(unreliable);
			org.apache.hadoop.io.retry.TestFailoverProxy.ConcurrentMethodThread t2 = new org.apache.hadoop.io.retry.TestFailoverProxy.ConcurrentMethodThread
				(unreliable);
			t1.start();
			t2.start();
			t1.join();
			t2.join();
			NUnit.Framework.Assert.AreEqual("impl2", t1.result);
			NUnit.Framework.Assert.AreEqual("impl2", t2.result);
			NUnit.Framework.Assert.AreEqual(1, proxyProvider.getFailoversOccurred());
		}

		/// <summary>
		/// Ensure that when all configured services are throwing StandbyException
		/// that we fail over back and forth between them until one is no longer
		/// throwing StandbyException.
		/// </summary>
		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		/// <exception cref="org.apache.hadoop.ipc.StandbyException"/>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testFailoverBetweenMultipleStandbys()
		{
			long millisToSleep = 10000;
			org.apache.hadoop.io.retry.UnreliableImplementation impl1 = new org.apache.hadoop.io.retry.UnreliableImplementation
				("impl1", org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.STANDBY_EXCEPTION);
			org.apache.hadoop.io.retry.TestFailoverProxy.FlipFlopProxyProvider<org.apache.hadoop.io.retry.UnreliableInterface
				> proxyProvider = new org.apache.hadoop.io.retry.TestFailoverProxy.FlipFlopProxyProvider
				<org.apache.hadoop.io.retry.UnreliableInterface>(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.retry.UnreliableInterface)), impl1, new org.apache.hadoop.io.retry.UnreliableImplementation
				("impl2", org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.STANDBY_EXCEPTION));
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(proxyProvider, org.apache.hadoop.io.retry.RetryPolicies.failoverOnNetworkException
				(org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL, 10, 1000, 10000));
			new _Thread_328(millisToSleep, impl1).start();
			string result = unreliable.failsIfIdentifierDoesntMatch("renamed-impl1");
			NUnit.Framework.Assert.AreEqual("renamed-impl1", result);
		}

		private sealed class _Thread_328 : java.lang.Thread
		{
			public _Thread_328(long millisToSleep, org.apache.hadoop.io.retry.UnreliableImplementation
				 impl1)
			{
				this.millisToSleep = millisToSleep;
				this.impl1 = impl1;
			}

			public override void run()
			{
				org.apache.hadoop.util.ThreadUtil.sleepAtLeastIgnoreInterrupts(millisToSleep);
				impl1.setIdentifier("renamed-impl1");
			}

			private readonly long millisToSleep;

			private readonly org.apache.hadoop.io.retry.UnreliableImplementation impl1;
		}

		/// <summary>Ensure that normal IO exceptions don't result in a failover.</summary>
		[NUnit.Framework.Test]
		public virtual void testExpectedIOException()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(newFlipFlopProxyProvider(org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.REMOTE_EXCEPTION, org.apache.hadoop.io.retry.UnreliableImplementation.TypeOfExceptionToFailWith
				.UNRELIABLE_EXCEPTION), org.apache.hadoop.io.retry.RetryPolicies.failoverOnNetworkException
				(org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL, 10, 1000, 10000));
			try
			{
				unreliable.failsIfIdentifierDoesntMatch("no-such-identifier");
				NUnit.Framework.Assert.Fail("Should have thrown *some* exception");
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.IsTrue("Expected IOE but got " + Sharpen.Runtime.getClassForObject
					(e), e is System.IO.IOException);
			}
		}
	}
}
