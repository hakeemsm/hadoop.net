using Sharpen;

namespace org.apache.hadoop.ipc
{
	public class TestFairCallQueue : NUnit.Framework.TestCase
	{
		private org.apache.hadoop.ipc.FairCallQueue<org.apache.hadoop.ipc.Schedulable> fcq;

		private org.apache.hadoop.ipc.Schedulable mockCall(string id)
		{
			org.apache.hadoop.ipc.Schedulable mockCall = org.mockito.Mockito.mock<org.apache.hadoop.ipc.Schedulable
				>();
			org.apache.hadoop.security.UserGroupInformation ugi = org.mockito.Mockito.mock<org.apache.hadoop.security.UserGroupInformation
				>();
			org.mockito.Mockito.when(ugi.getUserName()).thenReturn(id);
			org.mockito.Mockito.when(mockCall.getUserGroupInformation()).thenReturn(ugi);
			return mockCall;
		}

		private org.apache.hadoop.ipc.RpcScheduler alwaysZeroScheduler;

		// A scheduler which always schedules into priority zero
		// always queue 0
		protected override void setUp()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt("ns." + org.apache.hadoop.ipc.FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY
				, 2);
			fcq = new org.apache.hadoop.ipc.FairCallQueue<org.apache.hadoop.ipc.Schedulable>(
				5, "ns", conf);
		}

		//
		// Ensure that FairCallQueue properly implements BlockingQueue
		//
		public virtual void testPollReturnsNullWhenEmpty()
		{
			NUnit.Framework.Assert.IsNull(fcq.poll());
		}

		public virtual void testPollReturnsTopCallWhenNotEmpty()
		{
			org.apache.hadoop.ipc.Schedulable call = mockCall("c");
			NUnit.Framework.Assert.IsTrue(fcq.offer(call));
			NUnit.Framework.Assert.AreEqual(call, fcq.poll());
			// Poll took it out so the fcq is empty
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
		}

		public virtual void testOfferSucceeds()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			for (int i = 0; i < 5; i++)
			{
				// We can fit 10 calls
				NUnit.Framework.Assert.IsTrue(fcq.offer(mockCall("c")));
			}
			NUnit.Framework.Assert.AreEqual(5, fcq.Count);
		}

		public virtual void testOfferFailsWhenFull()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			for (int i = 0; i < 5; i++)
			{
				NUnit.Framework.Assert.IsTrue(fcq.offer(mockCall("c")));
			}
			NUnit.Framework.Assert.IsFalse(fcq.offer(mockCall("c")));
			// It's full
			NUnit.Framework.Assert.AreEqual(5, fcq.Count);
		}

		public virtual void testOfferSucceedsWhenScheduledLowPriority()
		{
			// Scheduler will schedule into queue 0 x 5, then queue 1
			org.apache.hadoop.ipc.RpcScheduler sched = org.mockito.Mockito.mock<org.apache.hadoop.ipc.RpcScheduler
				>();
			org.mockito.Mockito.when(sched.getPriorityLevel(org.mockito.Matchers.any<org.apache.hadoop.ipc.Schedulable
				>())).thenReturn(0, 0, 0, 0, 0, 1, 0);
			fcq.setScheduler(sched);
			for (int i = 0; i < 5; i++)
			{
				NUnit.Framework.Assert.IsTrue(fcq.offer(mockCall("c")));
			}
			NUnit.Framework.Assert.IsTrue(fcq.offer(mockCall("c")));
			NUnit.Framework.Assert.AreEqual(6, fcq.Count);
		}

		public virtual void testPeekNullWhenEmpty()
		{
			NUnit.Framework.Assert.IsNull(fcq.peek());
		}

		public virtual void testPeekNonDestructive()
		{
			org.apache.hadoop.ipc.Schedulable call = mockCall("c");
			NUnit.Framework.Assert.IsTrue(fcq.offer(call));
			NUnit.Framework.Assert.AreEqual(call, fcq.peek());
			NUnit.Framework.Assert.AreEqual(call, fcq.peek());
			// Non-destructive
			NUnit.Framework.Assert.AreEqual(1, fcq.Count);
		}

		public virtual void testPeekPointsAtHead()
		{
			org.apache.hadoop.ipc.Schedulable call = mockCall("c");
			org.apache.hadoop.ipc.Schedulable next = mockCall("b");
			fcq.offer(call);
			fcq.offer(next);
			NUnit.Framework.Assert.AreEqual(call, fcq.peek());
		}

		// Peek points at the head
		/// <exception cref="System.Exception"/>
		public virtual void testPollTimeout()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			NUnit.Framework.Assert.IsNull(fcq.poll(10, java.util.concurrent.TimeUnit.MILLISECONDS
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testPollSuccess()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			org.apache.hadoop.ipc.Schedulable call = mockCall("c");
			NUnit.Framework.Assert.IsTrue(fcq.offer(call));
			NUnit.Framework.Assert.AreEqual(call, fcq.poll(10, java.util.concurrent.TimeUnit.
				MILLISECONDS));
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testOfferTimeout()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			for (int i = 0; i < 5; i++)
			{
				NUnit.Framework.Assert.IsTrue(fcq.offer(mockCall("c"), 10, java.util.concurrent.TimeUnit
					.MILLISECONDS));
			}
			NUnit.Framework.Assert.IsFalse(fcq.offer(mockCall("e"), 10, java.util.concurrent.TimeUnit
				.MILLISECONDS));
			// It's full
			NUnit.Framework.Assert.AreEqual(5, fcq.Count);
		}

		public virtual void testDrainTo()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt("ns." + org.apache.hadoop.ipc.FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY
				, 2);
			org.apache.hadoop.ipc.FairCallQueue<org.apache.hadoop.ipc.Schedulable> fcq2 = new 
				org.apache.hadoop.ipc.FairCallQueue<org.apache.hadoop.ipc.Schedulable>(10, "ns", 
				conf);
			fcq.setScheduler(alwaysZeroScheduler);
			fcq2.setScheduler(alwaysZeroScheduler);
			// Start with 3 in fcq, to be drained
			for (int i = 0; i < 3; i++)
			{
				fcq.offer(mockCall("c"));
			}
			fcq.drainTo(fcq2);
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
			NUnit.Framework.Assert.AreEqual(3, fcq2.Count);
		}

		public virtual void testDrainToWithLimit()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt("ns." + org.apache.hadoop.ipc.FairCallQueue.IPC_CALLQUEUE_PRIORITY_LEVELS_KEY
				, 2);
			org.apache.hadoop.ipc.FairCallQueue<org.apache.hadoop.ipc.Schedulable> fcq2 = new 
				org.apache.hadoop.ipc.FairCallQueue<org.apache.hadoop.ipc.Schedulable>(10, "ns", 
				conf);
			fcq.setScheduler(alwaysZeroScheduler);
			fcq2.setScheduler(alwaysZeroScheduler);
			// Start with 3 in fcq, to be drained
			for (int i = 0; i < 3; i++)
			{
				fcq.offer(mockCall("c"));
			}
			fcq.drainTo(fcq2, 2);
			NUnit.Framework.Assert.AreEqual(1, fcq.Count);
			NUnit.Framework.Assert.AreEqual(2, fcq2.Count);
		}

		public virtual void testInitialRemainingCapacity()
		{
			NUnit.Framework.Assert.AreEqual(10, fcq.remainingCapacity());
		}

		public virtual void testFirstQueueFullRemainingCapacity()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			while (fcq.offer(mockCall("c")))
			{
			}
			// Queue 0 will fill up first, then queue 1
			NUnit.Framework.Assert.AreEqual(5, fcq.remainingCapacity());
		}

		public virtual void testAllQueuesFullRemainingCapacity()
		{
			org.apache.hadoop.ipc.RpcScheduler sched = org.mockito.Mockito.mock<org.apache.hadoop.ipc.RpcScheduler
				>();
			org.mockito.Mockito.when(sched.getPriorityLevel(org.mockito.Matchers.any<org.apache.hadoop.ipc.Schedulable
				>())).thenReturn(0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
			fcq.setScheduler(sched);
			while (fcq.offer(mockCall("c")))
			{
			}
			NUnit.Framework.Assert.AreEqual(0, fcq.remainingCapacity());
			NUnit.Framework.Assert.AreEqual(10, fcq.Count);
		}

		public virtual void testQueuesPartialFilledRemainingCapacity()
		{
			org.apache.hadoop.ipc.RpcScheduler sched = org.mockito.Mockito.mock<org.apache.hadoop.ipc.RpcScheduler
				>();
			org.mockito.Mockito.when(sched.getPriorityLevel(org.mockito.Matchers.any<org.apache.hadoop.ipc.Schedulable
				>())).thenReturn(0, 1, 0, 1, 0);
			fcq.setScheduler(sched);
			for (int i = 0; i < 5; i++)
			{
				fcq.offer(mockCall("c"));
			}
			NUnit.Framework.Assert.AreEqual(5, fcq.remainingCapacity());
			NUnit.Framework.Assert.AreEqual(5, fcq.Count);
		}

		/// <summary>Putter produces FakeCalls</summary>
		public class Putter : java.lang.Runnable
		{
			private readonly java.util.concurrent.BlockingQueue<org.apache.hadoop.ipc.Schedulable
				> cq;

			public readonly string tag;

			public volatile int callsAdded = 0;

			private readonly int maxCalls;

			private readonly java.util.concurrent.CountDownLatch latch;

			public Putter(TestFairCallQueue _enclosing, java.util.concurrent.BlockingQueue<org.apache.hadoop.ipc.Schedulable
				> aCq, int maxCalls, string tag, java.util.concurrent.CountDownLatch latch)
			{
				this._enclosing = _enclosing;
				// How many calls we added, accurate unless interrupted
				this.maxCalls = maxCalls;
				this.cq = aCq;
				this.tag = tag;
				this.latch = latch;
			}

			private string getTag()
			{
				if (this.tag != null)
				{
					return this.tag;
				}
				return string.Empty;
			}

			public virtual void run()
			{
				try
				{
					// Fill up to max (which is infinite if maxCalls < 0)
					while (this.callsAdded < this.maxCalls || this.maxCalls < 0)
					{
						this.cq.put(this._enclosing.mockCall(this.getTag()));
						this.callsAdded++;
						this.latch.countDown();
					}
				}
				catch (System.Exception)
				{
					return;
				}
			}

			private readonly TestFairCallQueue _enclosing;
		}

		/// <summary>Taker consumes FakeCalls</summary>
		public class Taker : java.lang.Runnable
		{
			private readonly java.util.concurrent.BlockingQueue<org.apache.hadoop.ipc.Schedulable
				> cq;

			public readonly string tag;

			public volatile int callsTaken = 0;

			public volatile org.apache.hadoop.ipc.Schedulable lastResult = null;

			private readonly int maxCalls;

			private readonly java.util.concurrent.CountDownLatch latch;

			private org.apache.hadoop.ipc.IdentityProvider uip;

			public Taker(TestFairCallQueue _enclosing, java.util.concurrent.BlockingQueue<org.apache.hadoop.ipc.Schedulable
				> aCq, int maxCalls, string tag, java.util.concurrent.CountDownLatch latch)
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
				this.uip = new org.apache.hadoop.ipc.UserIdentityProvider();
				this.latch = latch;
			}

			public virtual void run()
			{
				try
				{
					// Take while we don't exceed maxCalls, or if maxCalls is undefined (< 0)
					while (this.callsTaken < this.maxCalls || this.maxCalls < 0)
					{
						org.apache.hadoop.ipc.Schedulable res = this.cq.take();
						string identity = this.uip.makeIdentity(res);
						if (this.tag != null && this.tag.Equals(identity))
						{
							// This call does not match our tag, we should put it back and try again
							this.cq.put(res);
						}
						else
						{
							this.callsTaken++;
							this.latch.countDown();
							this.lastResult = res;
						}
					}
				}
				catch (System.Exception)
				{
					return;
				}
			}

			private readonly TestFairCallQueue _enclosing;
		}

		// Assert we can take exactly the numberOfTakes
		/// <exception cref="System.Exception"/>
		public virtual void assertCanTake(java.util.concurrent.BlockingQueue<org.apache.hadoop.ipc.Schedulable
			> cq, int numberOfTakes, int takeAttempts)
		{
			java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch
				(numberOfTakes);
			org.apache.hadoop.ipc.TestFairCallQueue.Taker taker = new org.apache.hadoop.ipc.TestFairCallQueue.Taker
				(this, cq, takeAttempts, "default", latch);
			java.lang.Thread t = new java.lang.Thread(taker);
			t.start();
			latch.await();
			NUnit.Framework.Assert.AreEqual(numberOfTakes, taker.callsTaken);
			t.interrupt();
		}

		// Assert we can put exactly the numberOfPuts
		/// <exception cref="System.Exception"/>
		public virtual void assertCanPut(java.util.concurrent.BlockingQueue<org.apache.hadoop.ipc.Schedulable
			> cq, int numberOfPuts, int putAttempts)
		{
			java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch
				(numberOfPuts);
			org.apache.hadoop.ipc.TestFairCallQueue.Putter putter = new org.apache.hadoop.ipc.TestFairCallQueue.Putter
				(this, cq, putAttempts, null, latch);
			java.lang.Thread t = new java.lang.Thread(putter);
			t.start();
			latch.await();
			NUnit.Framework.Assert.AreEqual(numberOfPuts, putter.callsAdded);
			t.interrupt();
		}

		// Make sure put will overflow into lower queues when the top is full
		/// <exception cref="System.Exception"/>
		public virtual void testPutOverflows()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			// We can fit more than 5, even though the scheduler suggests the top queue
			assertCanPut(fcq, 8, 8);
			NUnit.Framework.Assert.AreEqual(8, fcq.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testPutBlocksWhenAllFull()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			assertCanPut(fcq, 10, 10);
			// Fill up
			NUnit.Framework.Assert.AreEqual(10, fcq.Count);
			// Put more which causes overflow
			assertCanPut(fcq, 0, 1);
		}

		// Will block
		/// <exception cref="System.Exception"/>
		public virtual void testTakeBlocksWhenEmpty()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			assertCanTake(fcq, 0, 1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testTakeRemovesCall()
		{
			fcq.setScheduler(alwaysZeroScheduler);
			org.apache.hadoop.ipc.Schedulable call = mockCall("c");
			fcq.offer(call);
			NUnit.Framework.Assert.AreEqual(call, fcq.take());
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testTakeTriesNextQueue()
		{
			// Make a FCQ filled with calls in q 1 but empty in q 0
			org.apache.hadoop.ipc.RpcScheduler q1Scheduler = org.mockito.Mockito.mock<org.apache.hadoop.ipc.RpcScheduler
				>();
			org.mockito.Mockito.when(q1Scheduler.getPriorityLevel(org.mockito.Matchers.any<org.apache.hadoop.ipc.Schedulable
				>())).thenReturn(1);
			fcq.setScheduler(q1Scheduler);
			// A mux which only draws from q 0
			org.apache.hadoop.ipc.RpcMultiplexer q0mux = org.mockito.Mockito.mock<org.apache.hadoop.ipc.RpcMultiplexer
				>();
			org.mockito.Mockito.when(q0mux.getAndAdvanceCurrentIndex()).thenReturn(0);
			fcq.setMultiplexer(q0mux);
			org.apache.hadoop.ipc.Schedulable call = mockCall("c");
			fcq.put(call);
			// Take from q1 even though mux said q0, since q0 empty
			NUnit.Framework.Assert.AreEqual(call, fcq.take());
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
		}

		public TestFairCallQueue()
		{
			{
				org.apache.hadoop.ipc.RpcScheduler sched = org.mockito.Mockito.mock<org.apache.hadoop.ipc.RpcScheduler
					>();
				org.mockito.Mockito.when(sched.getPriorityLevel(org.mockito.Matchers.any<org.apache.hadoop.ipc.Schedulable
					>())).thenReturn(0);
				alwaysZeroScheduler = sched;
			}
		}
	}
}
