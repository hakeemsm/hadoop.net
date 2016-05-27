using System;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Net.Unix
{
	public class TestDomainSocketWatcher
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestDomainSocketWatcher
			));

		private Exception trappedException = null;

		[SetUp]
		public virtual void Before()
		{
			Assume.AssumeTrue(DomainSocket.GetLoadingFailureReason() == null);
		}

		[TearDown]
		public virtual void After()
		{
			if (trappedException != null)
			{
				throw new InvalidOperationException("DomainSocketWatcher thread terminated with unexpected exception."
					, trappedException);
			}
		}

		/// <summary>Test that we can create a DomainSocketWatcher and then shut it down.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCreateShutdown()
		{
			DomainSocketWatcher watcher = NewDomainSocketWatcher(10000000);
			watcher.Close();
		}

		/// <summary>Test that we can get notifications out a DomainSocketWatcher.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeliverNotifications()
		{
			DomainSocketWatcher watcher = NewDomainSocketWatcher(10000000);
			DomainSocket[] pair = DomainSocket.Socketpair();
			CountDownLatch latch = new CountDownLatch(1);
			watcher.Add(pair[1], new _Handler_73(latch));
			pair[0].Close();
			latch.Await();
			watcher.Close();
		}

		private sealed class _Handler_73 : DomainSocketWatcher.Handler
		{
			public _Handler_73(CountDownLatch latch)
			{
				this.latch = latch;
			}

			public bool Handle(DomainSocket sock)
			{
				latch.CountDown();
				return true;
			}

			private readonly CountDownLatch latch;
		}

		/// <summary>Test that a java interruption can stop the watcher thread</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestInterruption()
		{
			DomainSocketWatcher watcher = NewDomainSocketWatcher(10);
			watcher.watcherThread.Interrupt();
			Uninterruptibles.JoinUninterruptibly(watcher.watcherThread);
			watcher.Close();
		}

		/// <summary>Test that domain sockets are closed when the watcher is closed.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCloseSocketOnWatcherClose()
		{
			DomainSocketWatcher watcher = NewDomainSocketWatcher(10000000);
			DomainSocket[] pair = DomainSocket.Socketpair();
			watcher.Add(pair[1], new _Handler_103());
			watcher.Close();
			Uninterruptibles.JoinUninterruptibly(watcher.watcherThread);
			NUnit.Framework.Assert.IsFalse(pair[1].IsOpen());
		}

		private sealed class _Handler_103 : DomainSocketWatcher.Handler
		{
			public _Handler_103()
			{
			}

			public bool Handle(DomainSocket sock)
			{
				return true;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestStress()
		{
			int SocketNum = 250;
			ReentrantLock Lock = new ReentrantLock();
			DomainSocketWatcher watcher = NewDomainSocketWatcher(10000000);
			AList<DomainSocket[]> pairs = new AList<DomainSocket[]>();
			AtomicInteger handled = new AtomicInteger(0);
			Sharpen.Thread adderThread = new Sharpen.Thread(new _Runnable_122(SocketNum, watcher
				, Lock, pairs, handled));
			Sharpen.Thread removerThread = new Sharpen.Thread(new _Runnable_149(handled, SocketNum
				, Lock, pairs, watcher));
			adderThread.Start();
			removerThread.Start();
			Uninterruptibles.JoinUninterruptibly(adderThread);
			Uninterruptibles.JoinUninterruptibly(removerThread);
			watcher.Close();
		}

		private sealed class _Runnable_122 : Runnable
		{
			public _Runnable_122(int SocketNum, DomainSocketWatcher watcher, ReentrantLock Lock
				, AList<DomainSocket[]> pairs, AtomicInteger handled)
			{
				this.SocketNum = SocketNum;
				this.watcher = watcher;
				this.Lock = Lock;
				this.pairs = pairs;
				this.handled = handled;
			}

			public void Run()
			{
				try
				{
					for (int i = 0; i < SocketNum; i++)
					{
						DomainSocket[] pair = DomainSocket.Socketpair();
						watcher.Add(pair[1], new _Handler_128(handled));
						Lock.Lock();
						try
						{
							pairs.AddItem(pair);
						}
						finally
						{
							Lock.Unlock();
						}
					}
				}
				catch (Exception e)
				{
					TestDomainSocketWatcher.Log.Error(e);
					throw new RuntimeException(e);
				}
			}

			private sealed class _Handler_128 : DomainSocketWatcher.Handler
			{
				public _Handler_128(AtomicInteger handled)
				{
					this.handled = handled;
				}

				public bool Handle(DomainSocket sock)
				{
					handled.IncrementAndGet();
					return true;
				}

				private readonly AtomicInteger handled;
			}

			private readonly int SocketNum;

			private readonly DomainSocketWatcher watcher;

			private readonly ReentrantLock Lock;

			private readonly AList<DomainSocket[]> pairs;

			private readonly AtomicInteger handled;
		}

		private sealed class _Runnable_149 : Runnable
		{
			public _Runnable_149(AtomicInteger handled, int SocketNum, ReentrantLock Lock, AList
				<DomainSocket[]> pairs, DomainSocketWatcher watcher)
			{
				this.handled = handled;
				this.SocketNum = SocketNum;
				this.Lock = Lock;
				this.pairs = pairs;
				this.watcher = watcher;
			}

			public void Run()
			{
				Random random = new Random();
				try
				{
					while (handled.Get() != SocketNum)
					{
						Lock.Lock();
						try
						{
							if (!pairs.IsEmpty())
							{
								int idx = random.Next(pairs.Count);
								DomainSocket[] pair = pairs.Remove(idx);
								if (random.NextBoolean())
								{
									pair[0].Close();
								}
								else
								{
									watcher.Remove(pair[1]);
								}
							}
						}
						finally
						{
							Lock.Unlock();
						}
					}
				}
				catch (Exception e)
				{
					TestDomainSocketWatcher.Log.Error(e);
					throw new RuntimeException(e);
				}
			}

			private readonly AtomicInteger handled;

			private readonly int SocketNum;

			private readonly ReentrantLock Lock;

			private readonly AList<DomainSocket[]> pairs;

			private readonly DomainSocketWatcher watcher;
		}

		/// <summary>
		/// Creates a new DomainSocketWatcher and tracks its thread for termination due
		/// to an unexpected exception.
		/// </summary>
		/// <remarks>
		/// Creates a new DomainSocketWatcher and tracks its thread for termination due
		/// to an unexpected exception.  At the end of each test, if there was an
		/// unexpected exception, then that exception is thrown to force a failure of
		/// the test.
		/// </remarks>
		/// <param name="interruptCheckPeriodMs">
		/// interrupt check period passed to
		/// DomainSocketWatcher
		/// </param>
		/// <returns>new DomainSocketWatcher</returns>
		/// <exception cref="System.Exception">if there is any failure</exception>
		private DomainSocketWatcher NewDomainSocketWatcher(int interruptCheckPeriodMs)
		{
			DomainSocketWatcher watcher = new DomainSocketWatcher(interruptCheckPeriodMs, GetType
				().Name);
			watcher.watcherThread.SetUncaughtExceptionHandler(new _UncaughtExceptionHandler_200
				(this));
			return watcher;
		}

		private sealed class _UncaughtExceptionHandler_200 : Sharpen.Thread.UncaughtExceptionHandler
		{
			public _UncaughtExceptionHandler_200(TestDomainSocketWatcher _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void UncaughtException(Sharpen.Thread thread, Exception t)
			{
				this._enclosing.trappedException = t;
			}

			private readonly TestDomainSocketWatcher _enclosing;
		}
	}
}
