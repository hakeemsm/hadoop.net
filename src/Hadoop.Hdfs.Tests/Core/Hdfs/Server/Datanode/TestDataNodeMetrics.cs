using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Javax.Management;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestDataNodeMetrics
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.TestDataNodeMetrics
			));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDataNodeMetrics()
		{
			Configuration conf = new HdfsConfiguration();
			SimulatedFSDataset.SetFactory(conf);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				long LongFileLen = int.MaxValue + 1L;
				DFSTestUtil.CreateFile(fs, new Path("/tmp.txt"), LongFileLen, (short)1, 1L);
				IList<DataNode> datanodes = cluster.GetDataNodes();
				NUnit.Framework.Assert.AreEqual(datanodes.Count, 1);
				DataNode datanode = datanodes[0];
				MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(datanode.GetMetrics().Name());
				MetricsAsserts.AssertCounter("BytesWritten", LongFileLen, rb);
				NUnit.Framework.Assert.IsTrue("Expected non-zero number of incremental block reports"
					, MetricsAsserts.GetLongCounter("IncrementalBlockReportsNumOps", rb) > 0);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSendDataPacketMetrics()
		{
			Configuration conf = new HdfsConfiguration();
			int interval = 1;
			conf.Set(DFSConfigKeys.DfsMetricsPercentilesIntervalsKey, string.Empty + interval
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				// Create and read a 1 byte file
				Path tmpfile = new Path("/tmp.txt");
				DFSTestUtil.CreateFile(fs, tmpfile, (long)1, (short)1, 1L);
				DFSTestUtil.ReadFile(fs, tmpfile);
				IList<DataNode> datanodes = cluster.GetDataNodes();
				NUnit.Framework.Assert.AreEqual(datanodes.Count, 1);
				DataNode datanode = datanodes[0];
				MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(datanode.GetMetrics().Name());
				// Expect 2 packets, 1 for the 1 byte read, 1 for the empty packet
				// signaling the end of the block
				MetricsAsserts.AssertCounter("SendDataPacketTransferNanosNumOps", (long)2, rb);
				MetricsAsserts.AssertCounter("SendDataPacketBlockedOnNetworkNanosNumOps", (long)2
					, rb);
				// Wait for at least 1 rollover
				Sharpen.Thread.Sleep((interval + 1) * 1000);
				// Check that the sendPacket percentiles rolled to non-zero values
				string sec = interval + "s";
				MetricsAsserts.AssertQuantileGauges("SendDataPacketBlockedOnNetworkNanos" + sec, 
					rb);
				MetricsAsserts.AssertQuantileGauges("SendDataPacketTransferNanos" + sec, rb);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReceivePacketMetrics()
		{
			Configuration conf = new HdfsConfiguration();
			int interval = 1;
			conf.Set(DFSConfigKeys.DfsMetricsPercentilesIntervalsKey, string.Empty + interval
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem fs = cluster.GetFileSystem();
				Path testFile = new Path("/testFlushNanosMetric.txt");
				FSDataOutputStream fout = fs.Create(testFile);
				fout.Write(new byte[1]);
				fout.Hsync();
				fout.Close();
				IList<DataNode> datanodes = cluster.GetDataNodes();
				DataNode datanode = datanodes[0];
				MetricsRecordBuilder dnMetrics = MetricsAsserts.GetMetrics(datanode.GetMetrics().
					Name());
				// Expect two flushes, 1 for the flush that occurs after writing, 
				// 1 that occurs on closing the data and metadata files.
				MetricsAsserts.AssertCounter("FlushNanosNumOps", 2L, dnMetrics);
				// Expect two syncs, one from the hsync, one on close.
				MetricsAsserts.AssertCounter("FsyncNanosNumOps", 2L, dnMetrics);
				// Wait for at least 1 rollover
				Sharpen.Thread.Sleep((interval + 1) * 1000);
				// Check the receivePacket percentiles that should be non-zero
				string sec = interval + "s";
				MetricsAsserts.AssertQuantileGauges("FlushNanos" + sec, dnMetrics);
				MetricsAsserts.AssertQuantileGauges("FsyncNanos" + sec, dnMetrics);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Tests that round-trip acks in a datanode write pipeline are correctly
		/// measured.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRoundTripAckMetric()
		{
			int datanodeCount = 2;
			int interval = 1;
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsMetricsPercentilesIntervalsKey, string.Empty + interval
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(datanodeCount
				).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				// Open a file and get the head of the pipeline
				Path testFile = new Path("/testRoundTripAckMetric.txt");
				FSDataOutputStream fsout = fs.Create(testFile, (short)datanodeCount);
				DFSOutputStream dout = (DFSOutputStream)fsout.GetWrappedStream();
				// Slow down the writes to catch the write pipeline
				dout.SetChunksPerPacket(5);
				dout.SetArtificialSlowdown(3000);
				fsout.Write(new byte[10000]);
				DatanodeInfo[] pipeline = null;
				int count = 0;
				while (pipeline == null && count < 5)
				{
					pipeline = dout.GetPipeline();
					System.Console.Out.WriteLine("Waiting for pipeline to be created.");
					Sharpen.Thread.Sleep(1000);
					count++;
				}
				// Get the head node that should be receiving downstream acks
				DatanodeInfo headInfo = pipeline[0];
				DataNode headNode = null;
				foreach (DataNode datanode in cluster.GetDataNodes())
				{
					if (datanode.GetDatanodeId().Equals(headInfo))
					{
						headNode = datanode;
						break;
					}
				}
				NUnit.Framework.Assert.IsNotNull("Could not find the head of the datanode write pipeline"
					, headNode);
				// Close the file and wait for the metrics to rollover
				Sharpen.Thread.Sleep((interval + 1) * 1000);
				// Check the ack was received
				MetricsRecordBuilder dnMetrics = MetricsAsserts.GetMetrics(headNode.GetMetrics().
					Name());
				NUnit.Framework.Assert.IsTrue("Expected non-zero number of acks", MetricsAsserts.GetLongCounter
					("PacketAckRoundTripTimeNanosNumOps", dnMetrics) > 0);
				MetricsAsserts.AssertQuantileGauges("PacketAckRoundTripTimeNanos" + interval + "s"
					, dnMetrics);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestTimeoutMetric()
		{
			Configuration conf = new HdfsConfiguration();
			Path path = new Path("/test");
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			IList<FSDataOutputStream> streams = Lists.NewArrayList();
			try
			{
				FSDataOutputStream @out = cluster.GetFileSystem().Create(path, (short)2);
				DataNodeFaultInjector injector = Org.Mockito.Mockito.Mock<DataNodeFaultInjector>(
					);
				Org.Mockito.Mockito.DoThrow(new IOException("mock IOException")).When(injector).WriteBlockAfterFlush
					();
				DataNodeFaultInjector.instance = injector;
				streams.AddItem(@out);
				@out.WriteBytes("old gs data\n");
				@out.Hflush();
				/* Test the metric. */
				MetricsRecordBuilder dnMetrics = MetricsAsserts.GetMetrics(cluster.GetDataNodes()
					[0].GetMetrics().Name());
				MetricsAsserts.AssertCounter("DatanodeNetworkErrors", 1L, dnMetrics);
				/* Test JMX datanode network counts. */
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=DataNode,name=DataNodeInfo"
					);
				object dnc = mbs.GetAttribute(mxbeanName, "DatanodeNetworkCounts");
				string allDnc = dnc.ToString();
				NUnit.Framework.Assert.IsTrue("expected to see loopback address", allDnc.IndexOf(
					"127.0.0.1") >= 0);
				NUnit.Framework.Assert.IsTrue("expected to see networkErrors", allDnc.IndexOf("networkErrors"
					) >= 0);
			}
			finally
			{
				IOUtils.Cleanup(Log, Sharpen.Collections.ToArray(streams, new IDisposable[0]));
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				DataNodeFaultInjector.instance = new DataNodeFaultInjector();
			}
		}

		/// <summary>
		/// This function ensures that writing causes TotalWritetime to increment
		/// and reading causes totalReadTime to move.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDataNodeTimeSpend()
		{
			Configuration conf = new HdfsConfiguration();
			SimulatedFSDataset.SetFactory(conf);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				IList<DataNode> datanodes = cluster.GetDataNodes();
				NUnit.Framework.Assert.AreEqual(datanodes.Count, 1);
				DataNode datanode = datanodes[0];
				MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(datanode.GetMetrics().Name());
				long LongFileLen = 1024 * 1024 * 10;
				long startWriteValue = MetricsAsserts.GetLongCounter("TotalWriteTime", rb);
				long startReadValue = MetricsAsserts.GetLongCounter("TotalReadTime", rb);
				for (int x = 0; x < 50; x++)
				{
					DFSTestUtil.CreateFile(fs, new Path("/time.txt." + x), LongFileLen, (short)1, Time
						.MonotonicNow());
				}
				for (int x_1 = 0; x_1 < 50; x_1++)
				{
					string s = DFSTestUtil.ReadFile(fs, new Path("/time.txt." + x_1));
				}
				MetricsRecordBuilder rbNew = MetricsAsserts.GetMetrics(datanode.GetMetrics().Name
					());
				long endWriteValue = MetricsAsserts.GetLongCounter("TotalWriteTime", rbNew);
				long endReadValue = MetricsAsserts.GetLongCounter("TotalReadTime", rbNew);
				NUnit.Framework.Assert.IsTrue(endReadValue > startReadValue);
				NUnit.Framework.Assert.IsTrue(endWriteValue > startWriteValue);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
