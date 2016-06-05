using System;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Runner.Notification;


namespace Org.Apache.Hadoop.Test
{
	public class TestTimedOutTestsListener
	{
		public class Deadlock
		{
			private CyclicBarrier barrier = new CyclicBarrier(6);

			public Deadlock()
			{
				TestTimedOutTestsListener.Deadlock.DeadlockThread[] dThreads = new TestTimedOutTestsListener.Deadlock.DeadlockThread
					[6];
				TestTimedOutTestsListener.Deadlock.Monitor a = new TestTimedOutTestsListener.Deadlock.Monitor
					(this, "a");
				TestTimedOutTestsListener.Deadlock.Monitor b = new TestTimedOutTestsListener.Deadlock.Monitor
					(this, "b");
				TestTimedOutTestsListener.Deadlock.Monitor c = new TestTimedOutTestsListener.Deadlock.Monitor
					(this, "c");
				dThreads[0] = new TestTimedOutTestsListener.Deadlock.DeadlockThread(this, "MThread-1"
					, a, b);
				dThreads[1] = new TestTimedOutTestsListener.Deadlock.DeadlockThread(this, "MThread-2"
					, b, c);
				dThreads[2] = new TestTimedOutTestsListener.Deadlock.DeadlockThread(this, "MThread-3"
					, c, a);
				Lock d = new ReentrantLock();
				Lock e = new ReentrantLock();
				Lock f = new ReentrantLock();
				dThreads[3] = new TestTimedOutTestsListener.Deadlock.DeadlockThread(this, "SThread-4"
					, d, e);
				dThreads[4] = new TestTimedOutTestsListener.Deadlock.DeadlockThread(this, "SThread-5"
					, e, f);
				dThreads[5] = new TestTimedOutTestsListener.Deadlock.DeadlockThread(this, "SThread-6"
					, f, d);
				// make them daemon threads so that the test will exit
				for (int i = 0; i < 6; i++)
				{
					dThreads[i].SetDaemon(true);
					dThreads[i].Start();
				}
			}

			internal class DeadlockThread : Thread
			{
				private Lock lock1 = null;

				private Lock lock2 = null;

				private TestTimedOutTestsListener.Deadlock.Monitor mon1 = null;

				private TestTimedOutTestsListener.Deadlock.Monitor mon2 = null;

				private bool useSync;

				internal DeadlockThread(Deadlock _enclosing, string name, Lock lock1, Lock lock2)
					: base(name)
				{
					this._enclosing = _enclosing;
					this.lock1 = lock1;
					this.lock2 = lock2;
					this.useSync = true;
				}

				internal DeadlockThread(Deadlock _enclosing, string name, TestTimedOutTestsListener.Deadlock.Monitor
					 mon1, TestTimedOutTestsListener.Deadlock.Monitor mon2)
					: base(name)
				{
					this._enclosing = _enclosing;
					this.mon1 = mon1;
					this.mon2 = mon2;
					this.useSync = false;
				}

				public override void Run()
				{
					if (this.useSync)
					{
						this.SyncLock();
					}
					else
					{
						this.MonitorLock();
					}
				}

				private void SyncLock()
				{
					this.lock1.Lock();
					try
					{
						try
						{
							this._enclosing.barrier.Await();
						}
						catch (Exception)
						{
						}
						this.GoSyncDeadlock();
					}
					finally
					{
						this.lock1.Unlock();
					}
				}

				private void GoSyncDeadlock()
				{
					try
					{
						this._enclosing.barrier.Await();
					}
					catch (Exception)
					{
					}
					this.lock2.Lock();
					throw new RuntimeException("should not reach here.");
				}

				private void MonitorLock()
				{
					lock (this.mon1)
					{
						try
						{
							this._enclosing.barrier.Await();
						}
						catch (Exception)
						{
						}
						this.GoMonitorDeadlock();
					}
				}

				private void GoMonitorDeadlock()
				{
					try
					{
						this._enclosing.barrier.Await();
					}
					catch (Exception)
					{
					}
					lock (this.mon2)
					{
						throw new RuntimeException(this.GetName() + " should not reach here.");
					}
				}

				private readonly Deadlock _enclosing;
			}

			internal class Monitor
			{
				internal string name;

				internal Monitor(Deadlock _enclosing, string name)
				{
					this._enclosing = _enclosing;
					this.name = name;
				}

				private readonly Deadlock _enclosing;
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestThreadDumpAndDeadlocks()
		{
			new TestTimedOutTestsListener.Deadlock();
			string s = null;
			while (true)
			{
				s = TimedOutTestsListener.BuildDeadlockInfo();
				if (s != null)
				{
					break;
				}
				Thread.Sleep(100);
			}
			Assert.Equal(3, CountStringOccurrences(s, "BLOCKED"));
			Failure failure = new Failure(null, new Exception(TimedOutTestsListener.TestTimedOutPrefix
				));
			StringWriter writer = new StringWriter();
			new TimedOutTestsListener(new PrintWriter(writer)).TestFailure(failure);
			string @out = writer.ToString();
			Assert.True(@out.Contains("THREAD DUMP"));
			Assert.True(@out.Contains("DEADLOCKS DETECTED"));
			System.Console.Out.WriteLine(@out);
		}

		private int CountStringOccurrences(string s, string substr)
		{
			int n = 0;
			int index = 0;
			while ((index = s.IndexOf(substr, index) + 1) != 0)
			{
				n++;
			}
			return n;
		}
	}
}
