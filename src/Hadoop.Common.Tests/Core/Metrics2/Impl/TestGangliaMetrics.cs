using System.Collections.Generic;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Annotation;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Sink.Ganglia;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	public class TestGangliaMetrics
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestMetricsSystemImpl));

		private readonly string[] expectedMetrics = new string[] { "test.s1rec.C1", "test.s1rec.G1"
			, "test.s1rec.Xxx", "test.s1rec.Yyy", "test.s1rec.S1NumOps", "test.s1rec.S1AvgTime"
			 };

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestTagsForPrefix()
		{
			ConfigBuilder cb = new ConfigBuilder().Add("test.sink.ganglia.tagsForPrefix.all", 
				"*").Add("test.sink.ganglia.tagsForPrefix.some", "NumActiveSinks, " + "NumActiveSources"
				).Add("test.sink.ganglia.tagsForPrefix.none", string.Empty);
			GangliaSink30 sink = new GangliaSink30();
			sink.Init(cb.Subset("test.sink.ganglia"));
			IList<MetricsTag> tags = new AList<MetricsTag>();
			tags.AddItem(new MetricsTag(MsInfo.Context, "all"));
			tags.AddItem(new MetricsTag(MsInfo.NumActiveSources, "foo"));
			tags.AddItem(new MetricsTag(MsInfo.NumActiveSinks, "bar"));
			tags.AddItem(new MetricsTag(MsInfo.NumAllSinks, "haa"));
			tags.AddItem(new MetricsTag(MsInfo.Hostname, "host"));
			ICollection<AbstractMetric> metrics = new HashSet<AbstractMetric>();
			MetricsRecord record = new MetricsRecordImpl(MsInfo.Context, (long)1, tags, metrics
				);
			StringBuilder sb = new StringBuilder();
			sink.AppendPrefix(record, sb);
			Assert.Equal(".NumActiveSources=foo.NumActiveSinks=bar.NumAllSinks=haa"
				, sb.ToString());
			tags.Set(0, new MetricsTag(MsInfo.Context, "some"));
			sb = new StringBuilder();
			sink.AppendPrefix(record, sb);
			Assert.Equal(".NumActiveSources=foo.NumActiveSinks=bar", sb.ToString
				());
			tags.Set(0, new MetricsTag(MsInfo.Context, "none"));
			sb = new StringBuilder();
			sink.AppendPrefix(record, sb);
			Assert.Equal(string.Empty, sb.ToString());
			tags.Set(0, new MetricsTag(MsInfo.Context, "nada"));
			sb = new StringBuilder();
			sink.AppendPrefix(record, sb);
			Assert.Equal(string.Empty, sb.ToString());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGangliaMetrics2()
		{
			ConfigBuilder cb = new ConfigBuilder().Add("default.period", 10).Add("test.sink.gsink30.context"
				, "test").Add("test.sink.gsink31.context", "test").Save(TestMetricsConfig.GetTestFilename
				("hadoop-metrics2-test"));
			// filter out only "test"
			// filter out only "test"
			MetricsSystemImpl ms = new MetricsSystemImpl("Test");
			ms.Start();
			TestGangliaMetrics.TestSource s1 = ms.Register("s1", "s1 desc", new TestGangliaMetrics.TestSource
				("s1rec"));
			s1.c1.Incr();
			s1.xxx.Incr();
			s1.g1.Set(2);
			s1.yyy.Incr(2);
			s1.s1.Add(0);
			int expectedCountFromGanglia30 = expectedMetrics.Length;
			int expectedCountFromGanglia31 = 2 * expectedMetrics.Length;
			// Setup test for GangliaSink30
			AbstractGangliaSink gsink30 = new GangliaSink30();
			gsink30.Init(cb.Subset("test"));
			TestGangliaMetrics.MockDatagramSocket mockds30 = new TestGangliaMetrics.MockDatagramSocket
				(this);
			GangliaMetricsTestHelper.SetDatagramSocket(gsink30, mockds30);
			// Setup test for GangliaSink31
			AbstractGangliaSink gsink31 = new GangliaSink31();
			gsink31.Init(cb.Subset("test"));
			TestGangliaMetrics.MockDatagramSocket mockds31 = new TestGangliaMetrics.MockDatagramSocket
				(this);
			GangliaMetricsTestHelper.SetDatagramSocket(gsink31, mockds31);
			// register the sinks
			ms.Register("gsink30", "gsink30 desc", gsink30);
			ms.Register("gsink31", "gsink31 desc", gsink31);
			ms.PublishMetricsNow();
			// publish the metrics
			ms.Stop();
			// check GanfliaSink30 data
			CheckMetrics(mockds30.GetCapturedSend(), expectedCountFromGanglia30);
			// check GanfliaSink31 data
			CheckMetrics(mockds31.GetCapturedSend(), expectedCountFromGanglia31);
		}

		// check the expected against the actual metrics
		private void CheckMetrics(IList<byte[]> bytearrlist, int expectedCount)
		{
			bool[] foundMetrics = new bool[expectedMetrics.Length];
			foreach (byte[] bytes in bytearrlist)
			{
				string binaryStr = Runtime.GetStringForBytes(bytes);
				for (int index = 0; index < expectedMetrics.Length; index++)
				{
					if (binaryStr.IndexOf(expectedMetrics[index]) >= 0)
					{
						foundMetrics[index] = true;
						break;
					}
				}
			}
			for (int index_1 = 0; index_1 < foundMetrics.Length; index_1++)
			{
				if (!foundMetrics[index_1])
				{
					Assert.True("Missing metrics: " + expectedMetrics[index_1], false
						);
				}
			}
			Assert.Equal("Mismatch in record count: ", expectedCount, bytearrlist
				.Count);
		}

		private class TestSource
		{
			internal MutableCounterLong c1;

			internal MutableCounterLong xxx;

			internal MutableGaugeLong g1;

			internal MutableGaugeLong yyy;

			[Metric]
			internal MutableRate s1;

			internal readonly MetricsRegistry registry;

			internal TestSource(string recName)
			{
				registry = new MetricsRegistry(recName);
			}
		}

		/// <summary>This class is used to capture data send to Ganglia servers.</summary>
		/// <remarks>
		/// This class is used to capture data send to Ganglia servers.
		/// Initial attempt was to use mockito to mock and capture but
		/// while testing figured out that mockito is keeping the reference
		/// to the byte array and since the sink code reuses the byte array
		/// hence all the captured byte arrays were pointing to one instance.
		/// </remarks>
		private class MockDatagramSocket : DatagramSocket
		{
			private AList<byte[]> capture;

			/// <exception cref="System.Net.Sockets.SocketException"/>
			public MockDatagramSocket(TestGangliaMetrics _enclosing)
			{
				this._enclosing = _enclosing;
				this.capture = new AList<byte[]>();
			}

			/* (non-Javadoc)
			* @see java.net.DatagramSocket#send(java.net.DatagramPacket)
			*/
			/// <exception cref="System.IO.IOException"/>
			public override void Send(DatagramPacket p)
			{
				// capture the byte arrays
				byte[] bytes = new byte[p.GetLength()];
				System.Array.Copy(p.GetData(), p.GetOffset(), bytes, 0, p.GetLength());
				this.capture.AddItem(bytes);
			}

			/// <returns>the captured byte arrays</returns>
			internal virtual AList<byte[]> GetCapturedSend()
			{
				return this.capture;
			}

			private readonly TestGangliaMetrics _enclosing;
		}
	}
}
