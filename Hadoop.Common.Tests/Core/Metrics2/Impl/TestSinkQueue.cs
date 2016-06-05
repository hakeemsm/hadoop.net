using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Test the half-blocking metrics sink queue</summary>
	public class TestSinkQueue
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestSinkQueue));

		/// <summary>Test common use case</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCommon()
		{
			SinkQueue<int> q = new SinkQueue<int>(2);
			q.Enqueue(1);
			Assert.Equal("queue front", 1, (int)q.Front());
			Assert.Equal("queue back", 1, (int)q.Back());
			Assert.Equal("element", 1, (int)q.Dequeue());
			Assert.True("should enqueue", q.Enqueue(2));
			q.Consume(new _Consumer_50());
			Assert.True("should enqueue", q.Enqueue(3));
			Assert.Equal("element", 3, (int)q.Dequeue());
			Assert.Equal("queue size", 0, q.Size());
			Assert.Equal("queue front", null, q.Front());
			Assert.Equal("queue back", null, q.Back());
		}

		private sealed class _Consumer_50 : SinkQueue.Consumer<int>
		{
			public _Consumer_50()
			{
			}

			public void Consume(int e)
			{
				Assert.Equal("element", 2, (int)e);
			}
		}

		/// <summary>Test blocking when queue is empty</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestEmptyBlocking()
		{
			TestEmptyBlocking(0);
			TestEmptyBlocking(100);
		}

		/// <exception cref="System.Exception"/>
		private void TestEmptyBlocking(int awhile)
		{
			SinkQueue<int> q = new SinkQueue<int>(2);
			Runnable trigger = Org.Mockito.Mockito.Mock<Runnable>();
			// try consuming emtpy equeue and blocking
			Thread t = new _Thread_75(q, trigger);
			t.Start();
			// Should work with or without sleep
			if (awhile > 0)
			{
				Thread.Sleep(awhile);
			}
			q.Enqueue(1);
			q.Enqueue(2);
			t.Join();
			Org.Mockito.Mockito.Verify(trigger).Run();
		}

		private sealed class _Thread_75 : Thread
		{
			public _Thread_75(SinkQueue<int> q, Runnable trigger)
			{
				this.q = q;
				this.trigger = trigger;
			}

			public override void Run()
			{
				try
				{
					Assert.Equal("element", 1, (int)q.Dequeue());
					q.Consume(new _Consumer_79(trigger));
				}
				catch (Exception e)
				{
					TestSinkQueue.Log.Warn("Interrupted", e);
				}
			}

			private sealed class _Consumer_79 : SinkQueue.Consumer<int>
			{
				public _Consumer_79(Runnable trigger)
				{
					this.trigger = trigger;
				}

				public void Consume(int e)
				{
					Assert.Equal("element", 2, (int)e);
					trigger.Run();
				}

				private readonly Runnable trigger;
			}

			private readonly SinkQueue<int> q;

			private readonly Runnable trigger;
		}

		/// <summary>Test nonblocking enqueue when queue is full</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFull()
		{
			SinkQueue<int> q = new SinkQueue<int>(1);
			q.Enqueue(1);
			Assert.True("should drop", !q.Enqueue(2));
			Assert.Equal("element", 1, (int)q.Dequeue());
			q.Enqueue(3);
			q.Consume(new _Consumer_114());
			Assert.Equal("queue size", 0, q.Size());
		}

		private sealed class _Consumer_114 : SinkQueue.Consumer<int>
		{
			public _Consumer_114()
			{
			}

			public void Consume(int e)
			{
				Assert.Equal("element", 3, (int)e);
			}
		}

		/// <summary>Test the consumeAll method</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConsumeAll()
		{
			int capacity = 64;
			// arbitrary
			SinkQueue<int> q = new SinkQueue<int>(capacity);
			for (int i = 0; i < capacity; ++i)
			{
				Assert.True("should enqueue", q.Enqueue(i));
			}
			Assert.True("should not enqueue", !q.Enqueue(capacity));
			Runnable trigger = Org.Mockito.Mockito.Mock<Runnable>();
			q.ConsumeAll(new _Consumer_136(trigger));
			Org.Mockito.Mockito.Verify(trigger, Org.Mockito.Mockito.Times(capacity)).Run();
		}

		private sealed class _Consumer_136 : SinkQueue.Consumer<int>
		{
			public _Consumer_136(Runnable trigger)
			{
				this.trigger = trigger;
				this.expected = 0;
			}

			private int expected;

			public void Consume(int e)
			{
				Assert.Equal("element", this.expected++, (int)e);
				trigger.Run();
			}

			private readonly Runnable trigger;
		}

		/// <summary>Test the consumer throwing exceptions</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConsumerException()
		{
			SinkQueue<int> q = new SinkQueue<int>(1);
			RuntimeException ex = new RuntimeException("expected");
			q.Enqueue(1);
			try
			{
				q.Consume(new _Consumer_157(ex));
			}
			catch (Exception expected)
			{
				NUnit.Framework.Assert.AreSame("consumer exception", ex, expected);
			}
			// The queue should be in consistent state after exception
			Assert.Equal("queue size", 1, q.Size());
			Assert.Equal("element", 1, (int)q.Dequeue());
		}

		private sealed class _Consumer_157 : SinkQueue.Consumer<int>
		{
			public _Consumer_157(RuntimeException ex)
			{
				this.ex = ex;
			}

			public void Consume(int e)
			{
				throw ex;
			}

			private readonly RuntimeException ex;
		}

		/// <summary>Test the clear method</summary>
		[Fact]
		public virtual void TestClear()
		{
			SinkQueue<int> q = new SinkQueue<int>(128);
			for (int i = 0; i < q.Capacity() + 97; ++i)
			{
				q.Enqueue(i);
			}
			Assert.Equal("queue size", q.Capacity(), q.Size());
			q.Clear();
			Assert.Equal("queue size", 0, q.Size());
		}

		/// <summary>Test consumers that take their time.</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestHangingConsumer()
		{
			SinkQueue<int> q = NewSleepingConsumerQueue(2, 1, 2);
			Assert.Equal("queue back", 2, (int)q.Back());
			Assert.True("should drop", !q.Enqueue(3));
			// should not block
			Assert.Equal("queue size", 2, q.Size());
			Assert.Equal("queue head", 1, (int)q.Front());
			Assert.Equal("queue back", 2, (int)q.Back());
		}

		/// <summary>Test concurrent consumer access, which is illegal</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConcurrentConsumers()
		{
			SinkQueue<int> q = NewSleepingConsumerQueue(2, 1);
			Assert.True("should enqueue", q.Enqueue(2));
			Assert.Equal("queue back", 2, (int)q.Back());
			Assert.True("should drop", !q.Enqueue(3));
			// should not block
			ShouldThrowCME(new _Fun_206(q));
			ShouldThrowCME(new _Fun_211(q));
			ShouldThrowCME(new _Fun_216(q));
			ShouldThrowCME(new _Fun_221(q));
			// The queue should still be in consistent state after all the exceptions
			Assert.Equal("queue size", 2, q.Size());
			Assert.Equal("queue front", 1, (int)q.Front());
			Assert.Equal("queue back", 2, (int)q.Back());
		}

		private sealed class _Fun_206 : TestSinkQueue.Fun
		{
			public _Fun_206(SinkQueue<int> q)
			{
				this.q = q;
			}

			public void Run()
			{
				q.Clear();
			}

			private readonly SinkQueue<int> q;
		}

		private sealed class _Fun_211 : TestSinkQueue.Fun
		{
			public _Fun_211(SinkQueue<int> q)
			{
				this.q = q;
			}

			/// <exception cref="System.Exception"/>
			public void Run()
			{
				q.Consume(null);
			}

			private readonly SinkQueue<int> q;
		}

		private sealed class _Fun_216 : TestSinkQueue.Fun
		{
			public _Fun_216(SinkQueue<int> q)
			{
				this.q = q;
			}

			/// <exception cref="System.Exception"/>
			public void Run()
			{
				q.ConsumeAll(null);
			}

			private readonly SinkQueue<int> q;
		}

		private sealed class _Fun_221 : TestSinkQueue.Fun
		{
			public _Fun_221(SinkQueue<int> q)
			{
				this.q = q;
			}

			/// <exception cref="System.Exception"/>
			public void Run()
			{
				q.Dequeue();
			}

			private readonly SinkQueue<int> q;
		}

		/// <exception cref="System.Exception"/>
		private void ShouldThrowCME(TestSinkQueue.Fun callback)
		{
			try
			{
				callback.Run();
			}
			catch (ConcurrentModificationException e)
			{
				Log.Info(e);
				return;
			}
			Log.Error("should've thrown CME");
			NUnit.Framework.Assert.Fail("should've thrown CME");
		}

		/// <exception cref="System.Exception"/>
		private SinkQueue<int> NewSleepingConsumerQueue(int capacity, params int[] values
			)
		{
			SinkQueue<int> q = new SinkQueue<int>(capacity);
			foreach (int i in values)
			{
				q.Enqueue(i);
			}
			CountDownLatch barrier = new CountDownLatch(1);
			Thread t = new _Thread_251(q, barrier);
			// causes failure without barrier
			// a long time
			t.SetName("Sleeping consumer");
			t.SetDaemon(true);
			// so jvm can exit
			t.Start();
			barrier.Await();
			Log.Debug("Returning new sleeping consumer queue");
			return q;
		}

		private sealed class _Thread_251 : Thread
		{
			public _Thread_251(SinkQueue<int> q, CountDownLatch barrier)
			{
				this.q = q;
				this.barrier = barrier;
			}

			public override void Run()
			{
				try
				{
					Thread.Sleep(10);
					q.Consume(new _Consumer_255(barrier));
				}
				catch (Exception ex)
				{
					TestSinkQueue.Log.Warn("Interrupted", ex);
				}
			}

			private sealed class _Consumer_255 : SinkQueue.Consumer<int>
			{
				public _Consumer_255(CountDownLatch barrier)
				{
					this.barrier = barrier;
				}

				/// <exception cref="System.Exception"/>
				public void Consume(int e)
				{
					TestSinkQueue.Log.Info("sleeping");
					barrier.CountDown();
					Thread.Sleep(1000 * 86400);
				}

				private readonly CountDownLatch barrier;
			}

			private readonly SinkQueue<int> q;

			private readonly CountDownLatch barrier;
		}

		internal interface Fun
		{
			/// <exception cref="System.Exception"/>
			void Run();
		}
	}
}
