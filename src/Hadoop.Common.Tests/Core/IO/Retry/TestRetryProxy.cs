using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.Ipc;

using Reflect;

namespace Org.Apache.Hadoop.IO.Retry
{
	public class TestRetryProxy
	{
		private UnreliableImplementation unreliableImpl;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			unreliableImpl = new UnreliableImplementation();
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[Fact]
		public virtual void TestTryOnceThenFail()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.TryOnceThenFail);
			unreliable.AlwaysSucceeds();
			try
			{
				unreliable.FailsOnceThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <summary>
		/// Test for
		/// <see cref="RetryInvocationHandler{T}.IsRpcInvocation(object)"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRpcInvocation()
		{
			// For a proxy method should return true
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.RetryForever);
			Assert.True(RetryInvocationHandler.IsRpcInvocation(unreliable));
			// Embed the proxy in ProtocolTranslator
			ProtocolTranslator xlator = new _ProtocolTranslator_83(unreliable);
			// For a proxy wrapped in ProtocolTranslator method should return true
			Assert.True(RetryInvocationHandler.IsRpcInvocation(xlator));
			// Ensure underlying proxy was looked at
			Assert.Equal(xlator.ToString(), "1");
			// For non-proxy the method must return false
			NUnit.Framework.Assert.IsFalse(RetryInvocationHandler.IsRpcInvocation(new object(
				)));
		}

		private sealed class _ProtocolTranslator_83 : ProtocolTranslator
		{
			public _ProtocolTranslator_83(UnreliableInterface unreliable)
			{
				this.unreliable = unreliable;
				this.count = 0;
			}

			internal int count;

			public object GetUnderlyingProxyObject()
			{
				this.count++;
				return unreliable;
			}

			public override string ToString()
			{
				return string.Empty + this.count;
			}

			private readonly UnreliableInterface unreliable;
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[Fact]
		public virtual void TestRetryForever()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.RetryForever);
			unreliable.AlwaysSucceeds();
			unreliable.FailsOnceThenSucceeds();
			unreliable.FailsTenTimesThenSucceeds();
		}

		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[Fact]
		public virtual void TestRetryUpToMaximumCountWithFixedSleep()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.RetryUpToMaximumCountWithFixedSleep(8, 1, TimeUnit
				.Nanoseconds));
			unreliable.AlwaysSucceeds();
			unreliable.FailsOnceThenSucceeds();
			try
			{
				unreliable.FailsTenTimesThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[Fact]
		public virtual void TestRetryUpToMaximumTimeWithFixedSleep()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.RetryUpToMaximumTimeWithFixedSleep(80, 10, TimeUnit
				.Nanoseconds));
			unreliable.AlwaysSucceeds();
			unreliable.FailsOnceThenSucceeds();
			try
			{
				unreliable.FailsTenTimesThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[Fact]
		public virtual void TestRetryUpToMaximumCountWithProportionalSleep()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.RetryUpToMaximumCountWithProportionalSleep(8, 1, 
				TimeUnit.Nanoseconds));
			unreliable.AlwaysSucceeds();
			unreliable.FailsOnceThenSucceeds();
			try
			{
				unreliable.FailsTenTimesThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[Fact]
		public virtual void TestExponentialRetry()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.ExponentialBackoffRetry(5, 1L, TimeUnit.Nanoseconds
				));
			unreliable.AlwaysSucceeds();
			unreliable.FailsOnceThenSucceeds();
			try
			{
				unreliable.FailsTenTimesThenSucceeds();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (UnreliableInterface.UnreliableException)
			{
			}
		}

		// expected
		/// <exception cref="Org.Apache.Hadoop.IO.Retry.UnreliableInterface.UnreliableException
		/// 	"/>
		[Fact]
		public virtual void TestRetryByException()
		{
			IDictionary<Type, RetryPolicy> exceptionToPolicyMap = Collections.SingletonMap<Type
				, RetryPolicy>(typeof(UnreliableInterface.FatalException), RetryPolicies.TryOnceThenFail
				);
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.RetryByException(RetryPolicies.RetryForever, exceptionToPolicyMap
				));
			unreliable.FailsOnceThenSucceeds();
			try
			{
				unreliable.AlwaysFailsWithFatalException();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (UnreliableInterface.FatalException)
			{
			}
		}

		// expected
		[Fact]
		public virtual void TestRetryByRemoteException()
		{
			IDictionary<Type, RetryPolicy> exceptionToPolicyMap = Collections.SingletonMap
				<Type, RetryPolicy>(typeof(UnreliableInterface.FatalException), RetryPolicies.TryOnceThenFail
				);
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.RetryByRemoteException(RetryPolicies.RetryForever
				, exceptionToPolicyMap));
			try
			{
				unreliable.AlwaysFailsWithRemoteFatalException();
				NUnit.Framework.Assert.Fail("Should fail");
			}
			catch (RemoteException)
			{
			}
		}

		// expected
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRetryInterruptible()
		{
			UnreliableInterface unreliable = (UnreliableInterface)RetryProxy.Create<UnreliableInterface
				>(unreliableImpl, RetryPolicies.RetryUpToMaximumTimeWithFixedSleep(10, 10, TimeUnit
				.Seconds));
			CountDownLatch latch = new CountDownLatch(1);
			AtomicReference<Thread> futureThread = new AtomicReference<Thread
				>();
			ExecutorService exec = Executors.NewSingleThreadExecutor();
			Future<Exception> future = exec.Submit(new _Callable_216(futureThread, latch, unreliable
				));
			latch.Await();
			Thread.Sleep(1000);
			// time to fail and sleep
			Assert.True(futureThread.Get().IsAlive());
			futureThread.Get().Interrupt();
			Exception e = future.Get(1, TimeUnit.Seconds);
			// should return immediately 
			NUnit.Framework.Assert.IsNotNull(e);
			Assert.Equal(typeof(Exception), e.GetType());
			Assert.Equal("sleep interrupted", e.Message);
		}

		private sealed class _Callable_216 : Callable<Exception>
		{
			public _Callable_216(AtomicReference<Thread> futureThread, CountDownLatch
				 latch, UnreliableInterface unreliable)
			{
				this.futureThread = futureThread;
				this.latch = latch;
				this.unreliable = unreliable;
			}

			/// <exception cref="System.Exception"/>
			public Exception Call()
			{
				futureThread.Set(Thread.CurrentThread());
				latch.CountDown();
				try
				{
					unreliable.AlwaysFailsWithFatalException();
				}
				catch (UndeclaredThrowableException ute)
				{
					return ute.InnerException;
				}
				return null;
			}

			private readonly AtomicReference<Thread> futureThread;

			private readonly CountDownLatch latch;

			private readonly UnreliableInterface unreliable;
		}
	}
}
