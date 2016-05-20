using Sharpen;

namespace org.apache.hadoop.io.retry
{
	public class TestRetryProxy
	{
		private org.apache.hadoop.io.retry.UnreliableImplementation unreliableImpl;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			unreliableImpl = new org.apache.hadoop.io.retry.UnreliableImplementation();
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void testTryOnceThenFail()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL);
			unreliable.alwaysSucceeds();
			try
			{
				unreliable.failsOnceThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <summary>
		/// Test for
		/// <see cref="RetryInvocationHandler{T}.isRpcInvocation(object)"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRpcInvocation()
		{
			// For a proxy method should return true
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.RETRY_FOREVER);
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.io.retry.RetryInvocationHandler.isRpcInvocation
				(unreliable));
			// Embed the proxy in ProtocolTranslator
			org.apache.hadoop.ipc.ProtocolTranslator xlator = new _ProtocolTranslator_83(unreliable
				);
			// For a proxy wrapped in ProtocolTranslator method should return true
			NUnit.Framework.Assert.IsTrue(org.apache.hadoop.io.retry.RetryInvocationHandler.isRpcInvocation
				(xlator));
			// Ensure underlying proxy was looked at
			NUnit.Framework.Assert.AreEqual(xlator.ToString(), "1");
			// For non-proxy the method must return false
			NUnit.Framework.Assert.IsFalse(org.apache.hadoop.io.retry.RetryInvocationHandler.
				isRpcInvocation(new object()));
		}

		private sealed class _ProtocolTranslator_83 : org.apache.hadoop.ipc.ProtocolTranslator
		{
			public _ProtocolTranslator_83(org.apache.hadoop.io.retry.UnreliableInterface unreliable
				)
			{
				this.unreliable = unreliable;
				this.count = 0;
			}

			internal int count;

			public object getUnderlyingProxyObject()
			{
				this.count++;
				return unreliable;
			}

			public override string ToString()
			{
				return string.Empty + this.count;
			}

			private readonly org.apache.hadoop.io.retry.UnreliableInterface unreliable;
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void testRetryForever()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.RETRY_FOREVER);
			unreliable.alwaysSucceeds();
			unreliable.failsOnceThenSucceeds();
			unreliable.failsTenTimesThenSucceeds();
		}

		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void testRetryUpToMaximumCountWithFixedSleep()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep
				(8, 1, java.util.concurrent.TimeUnit.NANOSECONDS));
			unreliable.alwaysSucceeds();
			unreliable.failsOnceThenSucceeds();
			try
			{
				unreliable.failsTenTimesThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void testRetryUpToMaximumTimeWithFixedSleep()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumTimeWithFixedSleep
				(80, 10, java.util.concurrent.TimeUnit.NANOSECONDS));
			unreliable.alwaysSucceeds();
			unreliable.failsOnceThenSucceeds();
			try
			{
				unreliable.failsTenTimesThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void testRetryUpToMaximumCountWithProportionalSleep()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithProportionalSleep
				(8, 1, java.util.concurrent.TimeUnit.NANOSECONDS));
			unreliable.alwaysSucceeds();
			unreliable.failsOnceThenSucceeds();
			try
			{
				unreliable.failsTenTimesThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void testExponentialRetry()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.exponentialBackoffRetry
				(5, 1L, java.util.concurrent.TimeUnit.NANOSECONDS));
			unreliable.alwaysSucceeds();
			unreliable.failsOnceThenSucceeds();
			try
			{
				unreliable.failsTenTimesThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="org.apache.hadoop.io.retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[NUnit.Framework.Test]
		public virtual void testRetryByException()
		{
			System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
				> exceptionToPolicyMap = java.util.Collections.singletonMap<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
				>(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.retry.UnreliableInterface.FatalException
				)), org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL);
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.retryByException(org.apache.hadoop.io.retry.RetryPolicies
				.RETRY_FOREVER, exceptionToPolicyMap));
			unreliable.failsOnceThenSucceeds();
			try
			{
				unreliable.alwaysFailsWithFatalException();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (org.apache.hadoop.io.retry.UnreliableInterface.FatalException)
			{
			}
		}

		// expected
		[NUnit.Framework.Test]
		public virtual void testRetryByRemoteException()
		{
			System.Collections.Generic.IDictionary<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
				> exceptionToPolicyMap = java.util.Collections.singletonMap<java.lang.Class, org.apache.hadoop.io.retry.RetryPolicy
				>(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.retry.UnreliableInterface.FatalException
				)), org.apache.hadoop.io.retry.RetryPolicies.TRY_ONCE_THEN_FAIL);
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.retryByRemoteException
				(org.apache.hadoop.io.retry.RetryPolicies.RETRY_FOREVER, exceptionToPolicyMap));
			try
			{
				unreliable.alwaysFailsWithRemoteFatalException();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (org.apache.hadoop.ipc.RemoteException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRetryInterruptible()
		{
			org.apache.hadoop.io.retry.UnreliableInterface unreliable = (org.apache.hadoop.io.retry.UnreliableInterface
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.io.retry.UnreliableInterface
				>(unreliableImpl, org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumTimeWithFixedSleep
				(10, 10, java.util.concurrent.TimeUnit.SECONDS));
			java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch
				(1);
			java.util.concurrent.atomic.AtomicReference<java.lang.Thread> futureThread = new 
				java.util.concurrent.atomic.AtomicReference<java.lang.Thread>();
			java.util.concurrent.ExecutorService exec = java.util.concurrent.Executors.newSingleThreadExecutor
				();
			java.util.concurrent.Future<System.Exception> future = exec.submit(new _Callable_216
				(futureThread, latch, unreliable));
			latch.await();
			java.lang.Thread.sleep(1000);
			// time to fail and sleep
			NUnit.Framework.Assert.IsTrue(futureThread.get().isAlive());
			futureThread.get().interrupt();
			System.Exception e = future.get(1, java.util.concurrent.TimeUnit.SECONDS);
			// should return immediately 
			NUnit.Framework.Assert.IsNotNull(e);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(System.Exception
				)), Sharpen.Runtime.getClassForObject(e));
			NUnit.Framework.Assert.AreEqual("sleep interrupted", e.Message);
		}

		private sealed class _Callable_216 : java.util.concurrent.Callable<System.Exception
			>
		{
			public _Callable_216(java.util.concurrent.atomic.AtomicReference<java.lang.Thread
				> futureThread, java.util.concurrent.CountDownLatch latch, org.apache.hadoop.io.retry.UnreliableInterface
				 unreliable)
			{
				this.futureThread = futureThread;
				this.latch = latch;
				this.unreliable = unreliable;
			}

			/// <exception cref="System.Exception"/>
			public System.Exception call()
			{
				futureThread.set(java.lang.Thread.currentThread());
				latch.countDown();
				try
				{
					unreliable.alwaysFailsWithFatalException();
				}
				catch (java.lang.reflect.UndeclaredThrowableException ute)
				{
					return ute.InnerException;
				}
				return null;
			}

			private readonly java.util.concurrent.atomic.AtomicReference<java.lang.Thread> futureThread;

			private readonly java.util.concurrent.CountDownLatch latch;

			private readonly org.apache.hadoop.io.retry.UnreliableInterface unreliable;
		}
	}
}
