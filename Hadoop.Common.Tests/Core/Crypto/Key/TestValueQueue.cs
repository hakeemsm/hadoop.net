using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Crypto.Key.Kms;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	public class TestValueQueue
	{
		private class FillInfo
		{
			internal readonly int num;

			internal readonly string key;

			internal FillInfo(int num, string key)
			{
				this.num = num;
				this.key = key;
			}
		}

		private class MockFiller : ValueQueue.QueueRefiller<string>
		{
			internal readonly LinkedBlockingQueue<TestValueQueue.FillInfo> fillCalls = new LinkedBlockingQueue
				<TestValueQueue.FillInfo>();

			/// <exception cref="System.IO.IOException"/>
			public virtual void FillQueueForKey(string keyName, Queue<string> keyQueue, int numValues
				)
			{
				fillCalls.AddItem(new TestValueQueue.FillInfo(numValues, keyName));
				for (int i = 0; i < numValues; i++)
				{
					keyQueue.AddItem("test");
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual TestValueQueue.FillInfo GetTop()
			{
				return fillCalls.Poll(500, TimeUnit.Milliseconds);
			}
		}

		/// <summary>Verifies that Queue is initially filled to "numInitValues"</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitFill()
		{
			TestValueQueue.MockFiller filler = new TestValueQueue.MockFiller();
			ValueQueue<string> vq = new ValueQueue<string>(10, 0.1f, 300, 1, ValueQueue.SyncGenerationPolicy
				.All, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.GetNext("k1"));
			NUnit.Framework.Assert.AreEqual(1, filler.GetTop().num);
			vq.Shutdown();
		}

		/// <summary>Verifies that Queue is initialized (Warmed-up) for provided keys</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWarmUp()
		{
			TestValueQueue.MockFiller filler = new TestValueQueue.MockFiller();
			ValueQueue<string> vq = new ValueQueue<string>(10, 0.5f, 300, 1, ValueQueue.SyncGenerationPolicy
				.All, filler);
			vq.InitializeQueuesForKeys("k1", "k2", "k3");
			TestValueQueue.FillInfo[] fillInfos = new TestValueQueue.FillInfo[] { filler.GetTop
				(), filler.GetTop(), filler.GetTop() };
			NUnit.Framework.Assert.AreEqual(5, fillInfos[0].num);
			NUnit.Framework.Assert.AreEqual(5, fillInfos[1].num);
			NUnit.Framework.Assert.AreEqual(5, fillInfos[2].num);
			NUnit.Framework.Assert.AreEqual(Sets.NewHashSet("k1", "k2", "k3"), Sets.NewHashSet
				(fillInfos[0].key, fillInfos[1].key, fillInfos[2].key));
			vq.Shutdown();
		}

		/// <summary>
		/// Verifies that the refill task is executed after "checkInterval" if
		/// num values below "lowWatermark"
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefill()
		{
			TestValueQueue.MockFiller filler = new TestValueQueue.MockFiller();
			ValueQueue<string> vq = new ValueQueue<string>(10, 0.1f, 300, 1, ValueQueue.SyncGenerationPolicy
				.All, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.GetNext("k1"));
			NUnit.Framework.Assert.AreEqual(1, filler.GetTop().num);
			// Trigger refill
			vq.GetNext("k1");
			NUnit.Framework.Assert.AreEqual(1, filler.GetTop().num);
			NUnit.Framework.Assert.AreEqual(10, filler.GetTop().num);
			vq.Shutdown();
		}

		/// <summary>
		/// Verifies that the No refill Happens after "checkInterval" if
		/// num values above "lowWatermark"
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoRefill()
		{
			TestValueQueue.MockFiller filler = new TestValueQueue.MockFiller();
			ValueQueue<string> vq = new ValueQueue<string>(10, 0.5f, 300, 1, ValueQueue.SyncGenerationPolicy
				.All, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.GetNext("k1"));
			NUnit.Framework.Assert.AreEqual(5, filler.GetTop().num);
			NUnit.Framework.Assert.AreEqual(null, filler.GetTop());
			vq.Shutdown();
		}

		/// <summary>Verify getAtMost when SyncGeneration Policy = ALL</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestgetAtMostPolicyALL()
		{
			TestValueQueue.MockFiller filler = new TestValueQueue.MockFiller();
			ValueQueue<string> vq = new ValueQueue<string>(10, 0.1f, 300, 1, ValueQueue.SyncGenerationPolicy
				.All, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.GetNext("k1"));
			NUnit.Framework.Assert.AreEqual(1, filler.GetTop().num);
			// Drain completely
			NUnit.Framework.Assert.AreEqual(10, vq.GetAtMost("k1", 10).Count);
			// Synchronous call
			NUnit.Framework.Assert.AreEqual(10, filler.GetTop().num);
			// Ask for more... return all
			NUnit.Framework.Assert.AreEqual(19, vq.GetAtMost("k1", 19).Count);
			// Synchronous call (No Async call since num > lowWatermark)
			NUnit.Framework.Assert.AreEqual(19, filler.GetTop().num);
			vq.Shutdown();
		}

		/// <summary>Verify getAtMost when SyncGeneration Policy = ALL</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestgetAtMostPolicyATLEAST_ONE()
		{
			TestValueQueue.MockFiller filler = new TestValueQueue.MockFiller();
			ValueQueue<string> vq = new ValueQueue<string>(10, 0.3f, 300, 1, ValueQueue.SyncGenerationPolicy
				.AtleastOne, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.GetNext("k1"));
			NUnit.Framework.Assert.AreEqual(3, filler.GetTop().num);
			// Drain completely
			NUnit.Framework.Assert.AreEqual(2, vq.GetAtMost("k1", 10).Count);
			// Asynch Refill call
			NUnit.Framework.Assert.AreEqual(10, filler.GetTop().num);
			vq.Shutdown();
		}

		/// <summary>Verify getAtMost when SyncGeneration Policy = LOW_WATERMARK</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestgetAtMostPolicyLOW_WATERMARK()
		{
			TestValueQueue.MockFiller filler = new TestValueQueue.MockFiller();
			ValueQueue<string> vq = new ValueQueue<string>(10, 0.3f, 300, 1, ValueQueue.SyncGenerationPolicy
				.LowWatermark, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.GetNext("k1"));
			NUnit.Framework.Assert.AreEqual(3, filler.GetTop().num);
			// Drain completely
			NUnit.Framework.Assert.AreEqual(3, vq.GetAtMost("k1", 10).Count);
			// Synchronous call
			NUnit.Framework.Assert.AreEqual(1, filler.GetTop().num);
			// Asynch Refill call
			NUnit.Framework.Assert.AreEqual(10, filler.GetTop().num);
			vq.Shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDrain()
		{
			TestValueQueue.MockFiller filler = new TestValueQueue.MockFiller();
			ValueQueue<string> vq = new ValueQueue<string>(10, 0.1f, 300, 1, ValueQueue.SyncGenerationPolicy
				.All, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.GetNext("k1"));
			NUnit.Framework.Assert.AreEqual(1, filler.GetTop().num);
			vq.Drain("k1");
			NUnit.Framework.Assert.IsNull(filler.GetTop());
			vq.Shutdown();
		}
	}
}
