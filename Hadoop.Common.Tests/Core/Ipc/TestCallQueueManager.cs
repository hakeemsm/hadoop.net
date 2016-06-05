using System;
using System.Collections.Generic;


namespace Org.Apache.Hadoop.Ipc
{
	public class TestCallQueueManager
	{
		private CallQueueManager<TestCallQueueManager.FakeCall> manager;

		public class FakeCall
		{
			public readonly int tag;

			public FakeCall(TestCallQueueManager _enclosing, int tag)
			{
				this._enclosing = _enclosing;
				// Can be used for unique identification
				this.tag = tag;
			}

			private readonly TestCallQueueManager _enclosing;
		}

		/// <summary>Putter produces FakeCalls</summary>
		public class Putter : Runnable
		{
			private readonly CallQueueManager<TestCallQueueManager.FakeCall> cq;

			public readonly int tag;

			public volatile int callsAdded = 0;

			private readonly int maxCalls;

			private volatile bool isRunning = true;

			public Putter(TestCallQueueManager _enclosing, CallQueueManager<TestCallQueueManager.FakeCall
				> aCq, int maxCalls, int tag)
			{
				this._enclosing = _enclosing;
				// How many calls we added, accurate unless interrupted
				this.maxCalls = maxCalls;
				this.cq = aCq;
				this.tag = tag;
			}

			public virtual void Run()
			{
				try
				{
					// Fill up to max (which is infinite if maxCalls < 0)
					while (this.isRunning && (this.callsAdded < this.maxCalls || this.maxCalls < 0))
					{
						this.cq.Put(new TestCallQueueManager.FakeCall(this, this.tag));
						this.callsAdded++;
					}
				}
				catch (Exception)
				{
					return;
				}
			}

			public virtual void Stop()
			{
				this.isRunning = false;
			}

			private readonly TestCallQueueManager _enclosing;
		}

		/// <summary>Taker consumes FakeCalls</summary>
		public class Taker : Runnable
		{
			private readonly CallQueueManager<TestCallQueueManager.FakeCall> cq;

			public readonly int tag;

			public volatile int callsTaken = 0;

			public volatile TestCallQueueManager.FakeCall lastResult = null;

			private readonly int maxCalls;

			public Taker(TestCallQueueManager _enclosing, CallQueueManager<TestCallQueueManager.FakeCall
				> aCq, int maxCalls, int tag)
			{
				this._enclosing = _enclosing;
				// if >= 0 means we will only take the matching tag, and put back
				// anything else
				// total calls taken, accurate if we aren't interrupted
				// the last thing we took
				// maximum calls to take
				this.maxCalls = maxCalls;
				this.cq = aCq;
				this.tag = tag;
			}

			public virtual void Run()
			{
				try
				{
					// Take while we don't exceed maxCalls, or if maxCalls is undefined (< 0)
					while (this.callsTaken < this.maxCalls || this.maxCalls < 0)
					{
						TestCallQueueManager.FakeCall res = this.cq.Take();
						if (this.tag >= 0 && res.tag != this.tag)
						{
							// This call does not match our tag, we should put it back and try again
							this.cq.Put(res);
						}
						else
						{
							this.callsTaken++;
							this.lastResult = res;
						}
					}
				}
				catch (Exception)
				{
					return;
				}
			}

			private readonly TestCallQueueManager _enclosing;
		}

		// Assert we can take exactly the numberOfTakes
		/// <exception cref="System.Exception"/>
		public virtual void AssertCanTake(CallQueueManager<TestCallQueueManager.FakeCall>
			 cq, int numberOfTakes, int takeAttempts)
		{
			TestCallQueueManager.Taker taker = new TestCallQueueManager.Taker(this, cq, takeAttempts
				, -1);
			Thread t = new Thread(taker);
			t.Start();
			t.Join(100);
			Assert.Equal(taker.callsTaken, numberOfTakes);
			t.Interrupt();
		}

		// Assert we can put exactly the numberOfPuts
		/// <exception cref="System.Exception"/>
		public virtual void AssertCanPut(CallQueueManager<TestCallQueueManager.FakeCall> 
			cq, int numberOfPuts, int putAttempts)
		{
			TestCallQueueManager.Putter putter = new TestCallQueueManager.Putter(this, cq, putAttempts
				, -1);
			Thread t = new Thread(putter);
			t.Start();
			t.Join(100);
			Assert.Equal(putter.callsAdded, numberOfPuts);
			t.Interrupt();
		}

		private static readonly Type queueClass = CallQueueManager.ConvertQueueClass<TestCallQueueManager.FakeCall
			>(typeof(LinkedBlockingQueue));

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCallQueueCapacity()
		{
			manager = new CallQueueManager<TestCallQueueManager.FakeCall>(queueClass, 10, string.Empty
				, null);
			AssertCanPut(manager, 10, 20);
		}

		// Will stop at 10 due to capacity
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestEmptyConsume()
		{
			manager = new CallQueueManager<TestCallQueueManager.FakeCall>(queueClass, 10, string.Empty
				, null);
			AssertCanTake(manager, 0, 1);
		}

		// Fails since it's empty
		/// <exception cref="System.Exception"/>
		public virtual void TestSwapUnderContention()
		{
			manager = new CallQueueManager<TestCallQueueManager.FakeCall>(queueClass, 5000, string.Empty
				, null);
			AList<TestCallQueueManager.Putter> producers = new AList<TestCallQueueManager.Putter
				>();
			AList<TestCallQueueManager.Taker> consumers = new AList<TestCallQueueManager.Taker
				>();
			Dictionary<Runnable, Thread> threads = new Dictionary<Runnable, Thread
				>();
			// Create putters and takers
			for (int i = 0; i < 50; i++)
			{
				TestCallQueueManager.Putter p = new TestCallQueueManager.Putter(this, manager, -1
					, -1);
				Thread pt = new Thread(p);
				producers.AddItem(p);
				threads[p] = pt;
				pt.Start();
			}
			for (int i_1 = 0; i_1 < 20; i_1++)
			{
				TestCallQueueManager.Taker t = new TestCallQueueManager.Taker(this, manager, -1, 
					-1);
				Thread tt = new Thread(t);
				consumers.AddItem(t);
				threads[t] = tt;
				tt.Start();
			}
			Thread.Sleep(10);
			for (int i_2 = 0; i_2 < 5; i_2++)
			{
				manager.SwapQueue(queueClass, 5000, string.Empty, null);
			}
			// Stop the producers
			foreach (TestCallQueueManager.Putter p_1 in producers)
			{
				p_1.Stop();
			}
			// Wait for consumers to wake up, then consume
			Thread.Sleep(2000);
			Assert.Equal(0, manager.Size());
			// Ensure no calls were dropped
			long totalCallsCreated = 0;
			foreach (TestCallQueueManager.Putter p_2 in producers)
			{
				threads[p_2].Interrupt();
			}
			foreach (TestCallQueueManager.Putter p_3 in producers)
			{
				threads[p_3].Join();
				totalCallsCreated += p_3.callsAdded;
			}
			long totalCallsConsumed = 0;
			foreach (TestCallQueueManager.Taker t_1 in consumers)
			{
				threads[t_1].Interrupt();
			}
			foreach (TestCallQueueManager.Taker t_2 in consumers)
			{
				threads[t_2].Join();
				totalCallsConsumed += t_2.callsTaken;
			}
			Assert.Equal(totalCallsConsumed, totalCallsCreated);
		}
	}
}
