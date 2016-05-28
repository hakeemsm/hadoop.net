using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Tests if DataNode process exits if all Block Pool services exit.</summary>
	public class TestDataNodeExit
	{
		private const long WaitTimeInMillis = 10;

		internal Configuration conf;

		internal MiniDFSCluster cluster = null;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, 100);
			conf.SetInt(DFSConfigKeys.DfsBytesPerChecksumKey, 100);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
				(3)).Build();
			for (int i = 0; i < 3; i++)
			{
				cluster.WaitActive(i);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		private void StopBPServiceThreads(int numStopThreads, DataNode dn)
		{
			BPOfferService[] bpoList = dn.GetAllBpOs();
			int expected = dn.GetBpOsCount() - numStopThreads;
			int index = numStopThreads - 1;
			while (index >= 0)
			{
				bpoList[index--].Stop();
			}
			int iterations = 3000;
			// Total 30 seconds MAX wait time
			while (dn.GetBpOsCount() != expected && iterations > 0)
			{
				Sharpen.Thread.Sleep(WaitTimeInMillis);
				iterations--;
			}
			NUnit.Framework.Assert.AreEqual("Mismatch in number of BPServices running", expected
				, dn.GetBpOsCount());
		}

		/// <summary>Test BPService Thread Exit</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBPServiceExit()
		{
			DataNode dn = cluster.GetDataNodes()[0];
			StopBPServiceThreads(1, dn);
			NUnit.Framework.Assert.IsTrue("DataNode should not exit", dn.IsDatanodeUp());
			StopBPServiceThreads(2, dn);
			NUnit.Framework.Assert.IsFalse("DataNode should exit", dn.IsDatanodeUp());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSendOOBToPeers()
		{
			DataNode dn = cluster.GetDataNodes()[0];
			DataXceiverServer spyXserver = Org.Mockito.Mockito.Spy(dn.GetXferServer());
			ArgumentNullException e = new ArgumentNullException();
			Org.Mockito.Mockito.DoThrow(e).When(spyXserver).SendOOBToPeers();
			dn.xserver = spyXserver;
			try
			{
				dn.Shutdown();
			}
			catch (Exception t)
			{
				NUnit.Framework.Assert.Fail("DataNode shutdown should not have thrown exception "
					 + t);
			}
		}
	}
}
