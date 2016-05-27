using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	public class TestFairCallQueue : TestCase
	{
		private FairCallQueue<Schedulable> fcq;

		private Schedulable MockCall(string id)
		{
			Schedulable mockCall = Org.Mockito.Mockito.Mock<Schedulable>();
			UserGroupInformation ugi = Org.Mockito.Mockito.Mock<UserGroupInformation>();
			Org.Mockito.Mockito.When(ugi.GetUserName()).ThenReturn(id);
			Org.Mockito.Mockito.When(mockCall.GetUserGroupInformation()).ThenReturn(ugi);
			return mockCall;
		}

		private RpcScheduler alwaysZeroScheduler;

		// A scheduler which always schedules into priority zero
		// always queue 0
		protected override void SetUp()
		{
			Configuration conf = new Configuration();
			conf.SetInt("ns." + FairCallQueue.IpcCallqueuePriorityLevelsKey, 2);
			fcq = new FairCallQueue<Schedulable>(5, "ns", conf);
		}

		//
		// Ensure that FairCallQueue properly implements BlockingQueue
		//
		public virtual void TestPollReturnsNullWhenEmpty()
		{
			NUnit.Framework.Assert.IsNull(fcq.Poll());
		}

		public virtual void TestPollReturnsTopCallWhenNotEmpty()
		{
			Schedulable call = MockCall("c");
			NUnit.Framework.Assert.IsTrue(fcq.Offer(call));
			NUnit.Framework.Assert.AreEqual(call, fcq.Poll());
			// Poll took it out so the fcq is empty
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
		}

		public virtual void TestOfferSucceeds()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			for (int i = 0; i < 5; i++)
			{
				// We can fit 10 calls
				NUnit.Framework.Assert.IsTrue(fcq.Offer(MockCall("c")));
			}
			NUnit.Framework.Assert.AreEqual(5, fcq.Count);
		}

		public virtual void TestOfferFailsWhenFull()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			for (int i = 0; i < 5; i++)
			{
				NUnit.Framework.Assert.IsTrue(fcq.Offer(MockCall("c")));
			}
			NUnit.Framework.Assert.IsFalse(fcq.Offer(MockCall("c")));
			// It's full
			NUnit.Framework.Assert.AreEqual(5, fcq.Count);
		}

		public virtual void TestOfferSucceedsWhenScheduledLowPriority()
		{
			// Scheduler will schedule into queue 0 x 5, then queue 1
			RpcScheduler sched = Org.Mockito.Mockito.Mock<RpcScheduler>();
			Org.Mockito.Mockito.When(sched.GetPriorityLevel(Matchers.Any<Schedulable>())).ThenReturn
				(0, 0, 0, 0, 0, 1, 0);
			fcq.SetScheduler(sched);
			for (int i = 0; i < 5; i++)
			{
				NUnit.Framework.Assert.IsTrue(fcq.Offer(MockCall("c")));
			}
			NUnit.Framework.Assert.IsTrue(fcq.Offer(MockCall("c")));
			NUnit.Framework.Assert.AreEqual(6, fcq.Count);
		}

		public virtual void TestPeekNullWhenEmpty()
		{
			NUnit.Framework.Assert.IsNull(fcq.Peek());
		}

		public virtual void TestPeekNonDestructive()
		{
			Schedulable call = MockCall("c");
			NUnit.Framework.Assert.IsTrue(fcq.Offer(call));
			NUnit.Framework.Assert.AreEqual(call, fcq.Peek());
			NUnit.Framework.Assert.AreEqual(call, fcq.Peek());
			// Non-destructive
			NUnit.Framework.Assert.AreEqual(1, fcq.Count);
		}

		public virtual void TestPeekPointsAtHead()
		{
			Schedulable call = MockCall("c");
			Schedulable next = MockCall("b");
			fcq.Offer(call);
			fcq.Offer(next);
			NUnit.Framework.Assert.AreEqual(call, fcq.Peek());
		}

		// Peek points at the head
		/// <exception cref="System.Exception"/>
		public virtual void TestPollTimeout()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			NUnit.Framework.Assert.IsNull(fcq.Poll(10, TimeUnit.Milliseconds));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPollSuccess()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			Schedulable call = MockCall("c");
			NUnit.Framework.Assert.IsTrue(fcq.Offer(call));
			NUnit.Framework.Assert.AreEqual(call, fcq.Poll(10, TimeUnit.Milliseconds));
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestOfferTimeout()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			for (int i = 0; i < 5; i++)
			{
				NUnit.Framework.Assert.IsTrue(fcq.Offer(MockCall("c"), 10, TimeUnit.Milliseconds)
					);
			}
			NUnit.Framework.Assert.IsFalse(fcq.Offer(MockCall("e"), 10, TimeUnit.Milliseconds
				));
			// It's full
			NUnit.Framework.Assert.AreEqual(5, fcq.Count);
		}

		public virtual void TestDrainTo()
		{
			Configuration conf = new Configuration();
			conf.SetInt("ns." + FairCallQueue.IpcCallqueuePriorityLevelsKey, 2);
			FairCallQueue<Schedulable> fcq2 = new FairCallQueue<Schedulable>(10, "ns", conf);
			fcq.SetScheduler(alwaysZeroScheduler);
			fcq2.SetScheduler(alwaysZeroScheduler);
			// Start with 3 in fcq, to be drained
			for (int i = 0; i < 3; i++)
			{
				fcq.Offer(MockCall("c"));
			}
			fcq.DrainTo(fcq2);
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
			NUnit.Framework.Assert.AreEqual(3, fcq2.Count);
		}

		public virtual void TestDrainToWithLimit()
		{
			Configuration conf = new Configuration();
			conf.SetInt("ns." + FairCallQueue.IpcCallqueuePriorityLevelsKey, 2);
			FairCallQueue<Schedulable> fcq2 = new FairCallQueue<Schedulable>(10, "ns", conf);
			fcq.SetScheduler(alwaysZeroScheduler);
			fcq2.SetScheduler(alwaysZeroScheduler);
			// Start with 3 in fcq, to be drained
			for (int i = 0; i < 3; i++)
			{
				fcq.Offer(MockCall("c"));
			}
			fcq.DrainTo(fcq2, 2);
			NUnit.Framework.Assert.AreEqual(1, fcq.Count);
			NUnit.Framework.Assert.AreEqual(2, fcq2.Count);
		}

		public virtual void TestInitialRemainingCapacity()
		{
			NUnit.Framework.Assert.AreEqual(10, fcq.RemainingCapacity());
		}

		public virtual void TestFirstQueueFullRemainingCapacity()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			while (fcq.Offer(MockCall("c")))
			{
			}
			// Queue 0 will fill up first, then queue 1
			NUnit.Framework.Assert.AreEqual(5, fcq.RemainingCapacity());
		}

		public virtual void TestAllQueuesFullRemainingCapacity()
		{
			RpcScheduler sched = Org.Mockito.Mockito.Mock<RpcScheduler>();
			Org.Mockito.Mockito.When(sched.GetPriorityLevel(Matchers.Any<Schedulable>())).ThenReturn
				(0, 0, 0, 0, 0, 1, 1, 1, 1, 1);
			fcq.SetScheduler(sched);
			while (fcq.Offer(MockCall("c")))
			{
			}
			NUnit.Framework.Assert.AreEqual(0, fcq.RemainingCapacity());
			NUnit.Framework.Assert.AreEqual(10, fcq.Count);
		}

		public virtual void TestQueuesPartialFilledRemainingCapacity()
		{
			RpcScheduler sched = Org.Mockito.Mockito.Mock<RpcScheduler>();
			Org.Mockito.Mockito.When(sched.GetPriorityLevel(Matchers.Any<Schedulable>())).ThenReturn
				(0, 1, 0, 1, 0);
			fcq.SetScheduler(sched);
			for (int i = 0; i < 5; i++)
			{
				fcq.Offer(MockCall("c"));
			}
			NUnit.Framework.Assert.AreEqual(5, fcq.RemainingCapacity());
			NUnit.Framework.Assert.AreEqual(5, fcq.Count);
		}

		/// <summary>Putter produces FakeCalls</summary>
		public class Putter : Runnable
		{
			private readonly BlockingQueue<Schedulable> cq;

			public readonly string tag;

			public volatile int callsAdded = 0;

			private readonly int maxCalls;

			private readonly CountDownLatch latch;

			public Putter(TestFairCallQueue _enclosing, BlockingQueue<Schedulable> aCq, int maxCalls
				, string tag, CountDownLatch latch)
			{
				this._enclosing = _enclosing;
				// How many calls we added, accurate unless interrupted
				this.maxCalls = maxCalls;
				this.cq = aCq;
				this.tag = tag;
				this.latch = latch;
			}

			private string GetTag()
			{
				if (this.tag != null)
				{
					return this.tag;
				}
				return string.Empty;
			}

			public virtual void Run()
			{
				try
				{
					// Fill up to max (which is infinite if maxCalls < 0)
					while (this.callsAdded < this.maxCalls || this.maxCalls < 0)
					{
						this.cq.Put(this._enclosing.MockCall(this.GetTag()));
						this.callsAdded++;
						this.latch.CountDown();
					}
				}
				catch (Exception)
				{
					return;
				}
			}

			private readonly TestFairCallQueue _enclosing;
		}

		/// <summary>Taker consumes FakeCalls</summary>
		public class Taker : Runnable
		{
			private readonly BlockingQueue<Schedulable> cq;

			public readonly string tag;

			public volatile int callsTaken = 0;

			public volatile Schedulable lastResult = null;

			private readonly int maxCalls;

			private readonly CountDownLatch latch;

			private IdentityProvider uip;

			public Taker(TestFairCallQueue _enclosing, BlockingQueue<Schedulable> aCq, int maxCalls
				, string tag, CountDownLatch latch)
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
				this.uip = new UserIdentityProvider();
				this.latch = latch;
			}

			public virtual void Run()
			{
				try
				{
					// Take while we don't exceed maxCalls, or if maxCalls is undefined (< 0)
					while (this.callsTaken < this.maxCalls || this.maxCalls < 0)
					{
						Schedulable res = this.cq.Take();
						string identity = this.uip.MakeIdentity(res);
						if (this.tag != null && this.tag.Equals(identity))
						{
							// This call does not match our tag, we should put it back and try again
							this.cq.Put(res);
						}
						else
						{
							this.callsTaken++;
							this.latch.CountDown();
							this.lastResult = res;
						}
					}
				}
				catch (Exception)
				{
					return;
				}
			}

			private readonly TestFairCallQueue _enclosing;
		}

		// Assert we can take exactly the numberOfTakes
		/// <exception cref="System.Exception"/>
		public virtual void AssertCanTake(BlockingQueue<Schedulable> cq, int numberOfTakes
			, int takeAttempts)
		{
			CountDownLatch latch = new CountDownLatch(numberOfTakes);
			TestFairCallQueue.Taker taker = new TestFairCallQueue.Taker(this, cq, takeAttempts
				, "default", latch);
			Sharpen.Thread t = new Sharpen.Thread(taker);
			t.Start();
			latch.Await();
			NUnit.Framework.Assert.AreEqual(numberOfTakes, taker.callsTaken);
			t.Interrupt();
		}

		// Assert we can put exactly the numberOfPuts
		/// <exception cref="System.Exception"/>
		public virtual void AssertCanPut(BlockingQueue<Schedulable> cq, int numberOfPuts, 
			int putAttempts)
		{
			CountDownLatch latch = new CountDownLatch(numberOfPuts);
			TestFairCallQueue.Putter putter = new TestFairCallQueue.Putter(this, cq, putAttempts
				, null, latch);
			Sharpen.Thread t = new Sharpen.Thread(putter);
			t.Start();
			latch.Await();
			NUnit.Framework.Assert.AreEqual(numberOfPuts, putter.callsAdded);
			t.Interrupt();
		}

		// Make sure put will overflow into lower queues when the top is full
		/// <exception cref="System.Exception"/>
		public virtual void TestPutOverflows()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			// We can fit more than 5, even though the scheduler suggests the top queue
			AssertCanPut(fcq, 8, 8);
			NUnit.Framework.Assert.AreEqual(8, fcq.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPutBlocksWhenAllFull()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			AssertCanPut(fcq, 10, 10);
			// Fill up
			NUnit.Framework.Assert.AreEqual(10, fcq.Count);
			// Put more which causes overflow
			AssertCanPut(fcq, 0, 1);
		}

		// Will block
		/// <exception cref="System.Exception"/>
		public virtual void TestTakeBlocksWhenEmpty()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			AssertCanTake(fcq, 0, 1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTakeRemovesCall()
		{
			fcq.SetScheduler(alwaysZeroScheduler);
			Schedulable call = MockCall("c");
			fcq.Offer(call);
			NUnit.Framework.Assert.AreEqual(call, fcq.Take());
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTakeTriesNextQueue()
		{
			// Make a FCQ filled with calls in q 1 but empty in q 0
			RpcScheduler q1Scheduler = Org.Mockito.Mockito.Mock<RpcScheduler>();
			Org.Mockito.Mockito.When(q1Scheduler.GetPriorityLevel(Matchers.Any<Schedulable>()
				)).ThenReturn(1);
			fcq.SetScheduler(q1Scheduler);
			// A mux which only draws from q 0
			RpcMultiplexer q0mux = Org.Mockito.Mockito.Mock<RpcMultiplexer>();
			Org.Mockito.Mockito.When(q0mux.GetAndAdvanceCurrentIndex()).ThenReturn(0);
			fcq.SetMultiplexer(q0mux);
			Schedulable call = MockCall("c");
			fcq.Put(call);
			// Take from q1 even though mux said q0, since q0 empty
			NUnit.Framework.Assert.AreEqual(call, fcq.Take());
			NUnit.Framework.Assert.AreEqual(0, fcq.Count);
		}

		public TestFairCallQueue()
		{
			{
				RpcScheduler sched = Org.Mockito.Mockito.Mock<RpcScheduler>();
				Org.Mockito.Mockito.When(sched.GetPriorityLevel(Matchers.Any<Schedulable>())).ThenReturn
					(0);
				alwaysZeroScheduler = sched;
			}
		}
	}
}
