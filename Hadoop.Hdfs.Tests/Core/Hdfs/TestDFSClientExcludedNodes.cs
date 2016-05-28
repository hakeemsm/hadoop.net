using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// These tests make sure that DFSClient excludes writing data to
	/// a DN properly in case of errors.
	/// </summary>
	public class TestDFSClientExcludedNodes
	{
		private MiniDFSCluster cluster;

		private Configuration conf;

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			cluster = null;
			conf = new HdfsConfiguration();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestExcludedNodes()
		{
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			FileSystem fs = cluster.GetFileSystem();
			Path filePath = new Path("/testExcludedNodes");
			// kill a datanode
			cluster.StopDataNode(AppendTestUtil.NextInt(3));
			OutputStream @out = fs.Create(filePath, true, 4096, (short)3, fs.GetDefaultBlockSize
				(filePath));
			@out.Write(20);
			try
			{
				@out.Close();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Single DN failure should not result in a block abort: \n"
					 + e.Message);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestExcludedNodesForgiveness()
		{
			// Forgive nodes in under 2.5s for this test case.
			conf.SetLong(DFSConfigKeys.DfsClientWriteExcludeNodesCacheExpiryInterval, 2500);
			// We'll be using a 512 bytes block size just for tests
			// so making sure the checksum bytes too match it.
			conf.SetInt("io.bytes.per.checksum", 512);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			IList<MiniDFSCluster.DataNodeProperties> props = cluster.dataNodes;
			FileSystem fs = cluster.GetFileSystem();
			Path filePath = new Path("/testForgivingExcludedNodes");
			// 256 bytes data chunk for writes
			byte[] bytes = new byte[256];
			for (int index = 0; index < bytes.Length; index++)
			{
				bytes[index] = (byte)('0');
			}
			// File with a 512 bytes block size
			FSDataOutputStream @out = fs.Create(filePath, true, 4096, (short)3, 512);
			// Write a block to all 3 DNs (2x256bytes).
			@out.Write(bytes);
			@out.Write(bytes);
			@out.Hflush();
			// Remove two DNs, to put them into the exclude list.
			MiniDFSCluster.DataNodeProperties two = cluster.StopDataNode(2);
			MiniDFSCluster.DataNodeProperties one = cluster.StopDataNode(1);
			// Write another block.
			// At this point, we have two nodes already in excluded list.
			@out.Write(bytes);
			@out.Write(bytes);
			@out.Hflush();
			// Bring back the older DNs, since they are gonna be forgiven only
			// afterwards of this previous block write.
			NUnit.Framework.Assert.AreEqual(true, cluster.RestartDataNode(one, true));
			NUnit.Framework.Assert.AreEqual(true, cluster.RestartDataNode(two, true));
			cluster.WaitActive();
			// Sleep for 5s, to let the excluded nodes be expired
			// from the excludes list (i.e. forgiven after the configured wait period).
			// [Sleeping just in case the restart of the DNs completed < 5s cause
			// otherwise, we'll end up quickly excluding those again.]
			ThreadUtil.SleepAtLeastIgnoreInterrupts(5000);
			// Terminate the last good DN, to assert that there's no
			// single-DN-available scenario, caused by not forgiving the other
			// two by now.
			cluster.StopDataNode(0);
			try
			{
				// Attempt writing another block, which should still pass
				// cause the previous two should have been forgiven by now,
				// while the last good DN added to excludes this time.
				@out.Write(bytes);
				@out.Hflush();
				@out.Close();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Excluded DataNodes should be forgiven after a while and "
					 + "not cause file writing exception of: '" + e.Message + "'");
			}
		}
	}
}
