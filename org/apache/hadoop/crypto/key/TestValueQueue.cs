using Sharpen;

namespace org.apache.hadoop.crypto.key
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

		private class MockFiller : org.apache.hadoop.crypto.key.kms.ValueQueue.QueueRefiller
			<string>
		{
			internal readonly java.util.concurrent.LinkedBlockingQueue<org.apache.hadoop.crypto.key.TestValueQueue.FillInfo
				> fillCalls = new java.util.concurrent.LinkedBlockingQueue<org.apache.hadoop.crypto.key.TestValueQueue.FillInfo
				>();

			/// <exception cref="System.IO.IOException"/>
			public virtual void fillQueueForKey(string keyName, java.util.Queue<string> keyQueue
				, int numValues)
			{
				fillCalls.add(new org.apache.hadoop.crypto.key.TestValueQueue.FillInfo(numValues, 
					keyName));
				for (int i = 0; i < numValues; i++)
				{
					keyQueue.add("test");
				}
			}

			/// <exception cref="System.Exception"/>
			public virtual org.apache.hadoop.crypto.key.TestValueQueue.FillInfo getTop()
			{
				return fillCalls.poll(500, java.util.concurrent.TimeUnit.MILLISECONDS);
			}
		}

		/// <summary>Verifies that Queue is initially filled to "numInitValues"</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInitFill()
		{
			org.apache.hadoop.crypto.key.TestValueQueue.MockFiller filler = new org.apache.hadoop.crypto.key.TestValueQueue.MockFiller
				();
			org.apache.hadoop.crypto.key.kms.ValueQueue<string> vq = new org.apache.hadoop.crypto.key.kms.ValueQueue
				<string>(10, 0.1f, 300, 1, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.ALL, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.getNext("k1"));
			NUnit.Framework.Assert.AreEqual(1, filler.getTop().num);
			vq.shutdown();
		}

		/// <summary>Verifies that Queue is initialized (Warmed-up) for provided keys</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testWarmUp()
		{
			org.apache.hadoop.crypto.key.TestValueQueue.MockFiller filler = new org.apache.hadoop.crypto.key.TestValueQueue.MockFiller
				();
			org.apache.hadoop.crypto.key.kms.ValueQueue<string> vq = new org.apache.hadoop.crypto.key.kms.ValueQueue
				<string>(10, 0.5f, 300, 1, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.ALL, filler);
			vq.initializeQueuesForKeys("k1", "k2", "k3");
			org.apache.hadoop.crypto.key.TestValueQueue.FillInfo[] fillInfos = new org.apache.hadoop.crypto.key.TestValueQueue.FillInfo
				[] { filler.getTop(), filler.getTop(), filler.getTop() };
			NUnit.Framework.Assert.AreEqual(5, fillInfos[0].num);
			NUnit.Framework.Assert.AreEqual(5, fillInfos[1].num);
			NUnit.Framework.Assert.AreEqual(5, fillInfos[2].num);
			NUnit.Framework.Assert.AreEqual(com.google.common.collect.Sets.newHashSet("k1", "k2"
				, "k3"), com.google.common.collect.Sets.newHashSet(fillInfos[0].key, fillInfos[1
				].key, fillInfos[2].key));
			vq.shutdown();
		}

		/// <summary>
		/// Verifies that the refill task is executed after "checkInterval" if
		/// num values below "lowWatermark"
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRefill()
		{
			org.apache.hadoop.crypto.key.TestValueQueue.MockFiller filler = new org.apache.hadoop.crypto.key.TestValueQueue.MockFiller
				();
			org.apache.hadoop.crypto.key.kms.ValueQueue<string> vq = new org.apache.hadoop.crypto.key.kms.ValueQueue
				<string>(10, 0.1f, 300, 1, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.ALL, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.getNext("k1"));
			NUnit.Framework.Assert.AreEqual(1, filler.getTop().num);
			// Trigger refill
			vq.getNext("k1");
			NUnit.Framework.Assert.AreEqual(1, filler.getTop().num);
			NUnit.Framework.Assert.AreEqual(10, filler.getTop().num);
			vq.shutdown();
		}

		/// <summary>
		/// Verifies that the No refill Happens after "checkInterval" if
		/// num values above "lowWatermark"
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNoRefill()
		{
			org.apache.hadoop.crypto.key.TestValueQueue.MockFiller filler = new org.apache.hadoop.crypto.key.TestValueQueue.MockFiller
				();
			org.apache.hadoop.crypto.key.kms.ValueQueue<string> vq = new org.apache.hadoop.crypto.key.kms.ValueQueue
				<string>(10, 0.5f, 300, 1, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.ALL, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.getNext("k1"));
			NUnit.Framework.Assert.AreEqual(5, filler.getTop().num);
			NUnit.Framework.Assert.AreEqual(null, filler.getTop());
			vq.shutdown();
		}

		/// <summary>Verify getAtMost when SyncGeneration Policy = ALL</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testgetAtMostPolicyALL()
		{
			org.apache.hadoop.crypto.key.TestValueQueue.MockFiller filler = new org.apache.hadoop.crypto.key.TestValueQueue.MockFiller
				();
			org.apache.hadoop.crypto.key.kms.ValueQueue<string> vq = new org.apache.hadoop.crypto.key.kms.ValueQueue
				<string>(10, 0.1f, 300, 1, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.ALL, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.getNext("k1"));
			NUnit.Framework.Assert.AreEqual(1, filler.getTop().num);
			// Drain completely
			NUnit.Framework.Assert.AreEqual(10, vq.getAtMost("k1", 10).Count);
			// Synchronous call
			NUnit.Framework.Assert.AreEqual(10, filler.getTop().num);
			// Ask for more... return all
			NUnit.Framework.Assert.AreEqual(19, vq.getAtMost("k1", 19).Count);
			// Synchronous call (No Async call since num > lowWatermark)
			NUnit.Framework.Assert.AreEqual(19, filler.getTop().num);
			vq.shutdown();
		}

		/// <summary>Verify getAtMost when SyncGeneration Policy = ALL</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testgetAtMostPolicyATLEAST_ONE()
		{
			org.apache.hadoop.crypto.key.TestValueQueue.MockFiller filler = new org.apache.hadoop.crypto.key.TestValueQueue.MockFiller
				();
			org.apache.hadoop.crypto.key.kms.ValueQueue<string> vq = new org.apache.hadoop.crypto.key.kms.ValueQueue
				<string>(10, 0.3f, 300, 1, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.ATLEAST_ONE, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.getNext("k1"));
			NUnit.Framework.Assert.AreEqual(3, filler.getTop().num);
			// Drain completely
			NUnit.Framework.Assert.AreEqual(2, vq.getAtMost("k1", 10).Count);
			// Asynch Refill call
			NUnit.Framework.Assert.AreEqual(10, filler.getTop().num);
			vq.shutdown();
		}

		/// <summary>Verify getAtMost when SyncGeneration Policy = LOW_WATERMARK</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testgetAtMostPolicyLOW_WATERMARK()
		{
			org.apache.hadoop.crypto.key.TestValueQueue.MockFiller filler = new org.apache.hadoop.crypto.key.TestValueQueue.MockFiller
				();
			org.apache.hadoop.crypto.key.kms.ValueQueue<string> vq = new org.apache.hadoop.crypto.key.kms.ValueQueue
				<string>(10, 0.3f, 300, 1, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.LOW_WATERMARK, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.getNext("k1"));
			NUnit.Framework.Assert.AreEqual(3, filler.getTop().num);
			// Drain completely
			NUnit.Framework.Assert.AreEqual(3, vq.getAtMost("k1", 10).Count);
			// Synchronous call
			NUnit.Framework.Assert.AreEqual(1, filler.getTop().num);
			// Asynch Refill call
			NUnit.Framework.Assert.AreEqual(10, filler.getTop().num);
			vq.shutdown();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDrain()
		{
			org.apache.hadoop.crypto.key.TestValueQueue.MockFiller filler = new org.apache.hadoop.crypto.key.TestValueQueue.MockFiller
				();
			org.apache.hadoop.crypto.key.kms.ValueQueue<string> vq = new org.apache.hadoop.crypto.key.kms.ValueQueue
				<string>(10, 0.1f, 300, 1, org.apache.hadoop.crypto.key.kms.ValueQueue.SyncGenerationPolicy
				.ALL, filler);
			NUnit.Framework.Assert.AreEqual("test", vq.getNext("k1"));
			NUnit.Framework.Assert.AreEqual(1, filler.getTop().num);
			vq.drain("k1");
			NUnit.Framework.Assert.IsNull(filler.getTop());
			vq.shutdown();
		}
	}
}
