using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Ipc
{
	public class TestWeightedRoundRobinMultiplexer
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestWeightedRoundRobinMultiplexer
			));

		private WeightedRoundRobinMultiplexer mux;

		public virtual void TestInstantiateNegativeMux()
		{
			mux = new WeightedRoundRobinMultiplexer(-1, string.Empty, new Configuration());
		}

		public virtual void TestInstantiateZeroMux()
		{
			mux = new WeightedRoundRobinMultiplexer(0, string.Empty, new Configuration());
		}

		public virtual void TestInstantiateIllegalMux()
		{
			Configuration conf = new Configuration();
			conf.SetStrings("namespace." + WeightedRoundRobinMultiplexer.IpcCallqueueWrrmuxWeightsKey
				, "1", "2", "3");
			// ask for 3 weights with 2 queues
			mux = new WeightedRoundRobinMultiplexer(2, "namespace", conf);
		}

		[Fact]
		public virtual void TestLegalInstantiation()
		{
			Configuration conf = new Configuration();
			conf.SetStrings("namespace." + WeightedRoundRobinMultiplexer.IpcCallqueueWrrmuxWeightsKey
				, "1", "2", "3");
			// ask for 3 weights with 3 queues
			mux = new WeightedRoundRobinMultiplexer(3, "namespace.", conf);
		}

		[Fact]
		public virtual void TestDefaultPattern()
		{
			// Mux of size 1: 0 0 0 0 0, etc
			mux = new WeightedRoundRobinMultiplexer(1, string.Empty, new Configuration());
			for (int i = 0; i < 10; i++)
			{
				Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			}
			// Mux of size 2: 0 0 1 0 0 1 0 0 1, etc
			mux = new WeightedRoundRobinMultiplexer(2, string.Empty, new Configuration());
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			// Size 3: 4x0 2x1 1x2, etc
			mux = new WeightedRoundRobinMultiplexer(3, string.Empty, new Configuration());
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 2);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			// Size 4: 8x0 4x1 2x2 1x3
			mux = new WeightedRoundRobinMultiplexer(4, string.Empty, new Configuration());
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 2);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 2);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 3);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
		}

		[Fact]
		public virtual void TestCustomPattern()
		{
			// 1x0 1x1
			Configuration conf = new Configuration();
			conf.SetStrings("test.custom." + WeightedRoundRobinMultiplexer.IpcCallqueueWrrmuxWeightsKey
				, "1", "1");
			mux = new WeightedRoundRobinMultiplexer(2, "test.custom", conf);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
			Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
			// 1x0 3x1 2x2
			conf.SetStrings("test.custom." + WeightedRoundRobinMultiplexer.IpcCallqueueWrrmuxWeightsKey
				, "1", "3", "2");
			mux = new WeightedRoundRobinMultiplexer(3, "test.custom", conf);
			for (int i = 0; i < 5; i++)
			{
				Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 0);
				Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
				Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
				Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 1);
				Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 2);
				Assert.Equal(mux.GetAndAdvanceCurrentIndex(), 2);
			}
		}
		// Ensure pattern repeats
	}
}
