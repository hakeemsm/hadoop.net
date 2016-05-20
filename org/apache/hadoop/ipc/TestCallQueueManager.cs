using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class TestCallQueueManager
	{
		private org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall
			> manager;

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
		public class Putter : java.lang.Runnable
		{
			private readonly org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall
				> cq;

			public readonly int tag;

			public volatile int callsAdded = 0;

			private readonly int maxCalls;

			private volatile bool isRunning = true;

			public Putter(TestCallQueueManager _enclosing, org.apache.hadoop.ipc.CallQueueManager
				<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall> aCq, int maxCalls, int tag
				)
			{
				this._enclosing = _enclosing;
				// How many calls we added, accurate unless interrupted
				this.maxCalls = maxCalls;
				this.cq = aCq;
				this.tag = tag;
			}

			public virtual void run()
			{
				try
				{
					// Fill up to max (which is infinite if maxCalls < 0)
					while (this.isRunning && (this.callsAdded < this.maxCalls || this.maxCalls < 0))
					{
						this.cq.put(new org.apache.hadoop.ipc.TestCallQueueManager.FakeCall(this, this.tag
							));
						this.callsAdded++;
					}
				}
				catch (System.Exception)
				{
					return;
				}
			}

			public virtual void stop()
			{
				this.isRunning = false;
			}

			private readonly TestCallQueueManager _enclosing;
		}

		/// <summary>Taker consumes FakeCalls</summary>
		public class Taker : java.lang.Runnable
		{
			private readonly org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall
				> cq;

			public readonly int tag;

			public volatile int callsTaken = 0;

			public volatile org.apache.hadoop.ipc.TestCallQueueManager.FakeCall lastResult = 
				null;

			private readonly int maxCalls;

			public Taker(TestCallQueueManager _enclosing, org.apache.hadoop.ipc.CallQueueManager
				<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall> aCq, int maxCalls, int tag
				)
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

			public virtual void run()
			{
				try
				{
					// Take while we don't exceed maxCalls, or if maxCalls is undefined (< 0)
					while (this.callsTaken < this.maxCalls || this.maxCalls < 0)
					{
						org.apache.hadoop.ipc.TestCallQueueManager.FakeCall res = this.cq.take();
						if (this.tag >= 0 && res.tag != this.tag)
						{
							// This call does not match our tag, we should put it back and try again
							this.cq.put(res);
						}
						else
						{
							this.callsTaken++;
							this.lastResult = res;
						}
					}
				}
				catch (System.Exception)
				{
					return;
				}
			}

			private readonly TestCallQueueManager _enclosing;
		}

		// Assert we can take exactly the numberOfTakes
		/// <exception cref="System.Exception"/>
		public virtual void assertCanTake(org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall
			> cq, int numberOfTakes, int takeAttempts)
		{
			org.apache.hadoop.ipc.TestCallQueueManager.Taker taker = new org.apache.hadoop.ipc.TestCallQueueManager.Taker
				(this, cq, takeAttempts, -1);
			java.lang.Thread t = new java.lang.Thread(taker);
			t.start();
			t.join(100);
			NUnit.Framework.Assert.AreEqual(taker.callsTaken, numberOfTakes);
			t.interrupt();
		}

		// Assert we can put exactly the numberOfPuts
		/// <exception cref="System.Exception"/>
		public virtual void assertCanPut(org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall
			> cq, int numberOfPuts, int putAttempts)
		{
			org.apache.hadoop.ipc.TestCallQueueManager.Putter putter = new org.apache.hadoop.ipc.TestCallQueueManager.Putter
				(this, cq, putAttempts, -1);
			java.lang.Thread t = new java.lang.Thread(putter);
			t.start();
			t.join(100);
			NUnit.Framework.Assert.AreEqual(putter.callsAdded, numberOfPuts);
			t.interrupt();
		}

		private static readonly java.lang.Class queueClass = org.apache.hadoop.ipc.CallQueueManager
			.convertQueueClass<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall>(Sharpen.Runtime.getClassForType
			(typeof(java.util.concurrent.LinkedBlockingQueue)));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCallQueueCapacity()
		{
			manager = new org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall
				>(queueClass, 10, string.Empty, null);
			assertCanPut(manager, 10, 20);
		}

		// Will stop at 10 due to capacity
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testEmptyConsume()
		{
			manager = new org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall
				>(queueClass, 10, string.Empty, null);
			assertCanTake(manager, 0, 1);
		}

		// Fails since it's empty
		/// <exception cref="System.Exception"/>
		public virtual void testSwapUnderContention()
		{
			manager = new org.apache.hadoop.ipc.CallQueueManager<org.apache.hadoop.ipc.TestCallQueueManager.FakeCall
				>(queueClass, 5000, string.Empty, null);
			System.Collections.Generic.List<org.apache.hadoop.ipc.TestCallQueueManager.Putter
				> producers = new System.Collections.Generic.List<org.apache.hadoop.ipc.TestCallQueueManager.Putter
				>();
			System.Collections.Generic.List<org.apache.hadoop.ipc.TestCallQueueManager.Taker>
				 consumers = new System.Collections.Generic.List<org.apache.hadoop.ipc.TestCallQueueManager.Taker
				>();
			System.Collections.Generic.Dictionary<java.lang.Runnable, java.lang.Thread> threads
				 = new System.Collections.Generic.Dictionary<java.lang.Runnable, java.lang.Thread
				>();
			// Create putters and takers
			for (int i = 0; i < 50; i++)
			{
				org.apache.hadoop.ipc.TestCallQueueManager.Putter p = new org.apache.hadoop.ipc.TestCallQueueManager.Putter
					(this, manager, -1, -1);
				java.lang.Thread pt = new java.lang.Thread(p);
				producers.add(p);
				threads[p] = pt;
				pt.start();
			}
			for (int i_1 = 0; i_1 < 20; i_1++)
			{
				org.apache.hadoop.ipc.TestCallQueueManager.Taker t = new org.apache.hadoop.ipc.TestCallQueueManager.Taker
					(this, manager, -1, -1);
				java.lang.Thread tt = new java.lang.Thread(t);
				consumers.add(t);
				threads[t] = tt;
				tt.start();
			}
			java.lang.Thread.sleep(10);
			for (int i_2 = 0; i_2 < 5; i_2++)
			{
				manager.swapQueue(queueClass, 5000, string.Empty, null);
			}
			// Stop the producers
			foreach (org.apache.hadoop.ipc.TestCallQueueManager.Putter p_1 in producers)
			{
				p_1.stop();
			}
			// Wait for consumers to wake up, then consume
			java.lang.Thread.sleep(2000);
			NUnit.Framework.Assert.AreEqual(0, manager.size());
			// Ensure no calls were dropped
			long totalCallsCreated = 0;
			foreach (org.apache.hadoop.ipc.TestCallQueueManager.Putter p_2 in producers)
			{
				threads[p_2].interrupt();
			}
			foreach (org.apache.hadoop.ipc.TestCallQueueManager.Putter p_3 in producers)
			{
				threads[p_3].join();
				totalCallsCreated += p_3.callsAdded;
			}
			long totalCallsConsumed = 0;
			foreach (org.apache.hadoop.ipc.TestCallQueueManager.Taker t_1 in consumers)
			{
				threads[t_1].interrupt();
			}
			foreach (org.apache.hadoop.ipc.TestCallQueueManager.Taker t_2 in consumers)
			{
				threads[t_2].join();
				totalCallsConsumed += t_2.callsTaken;
			}
			NUnit.Framework.Assert.AreEqual(totalCallsConsumed, totalCallsCreated);
		}
	}
}
