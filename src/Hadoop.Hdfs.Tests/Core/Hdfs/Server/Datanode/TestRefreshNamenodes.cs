using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Tests datanode refresh namenode list functionality.</summary>
	public class TestRefreshNamenodes
	{
		private readonly int nnPort1 = 2221;

		private readonly int nnPort2 = 2224;

		private readonly int nnPort3 = 2227;

		private readonly int nnPort4 = 2230;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshNamenodes()
		{
			// Start cluster with a single NN and DN
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
					("ns1").AddNN(new MiniDFSNNTopology.NNConf(null).SetIpcPort(nnPort1))).SetFederation
					(true);
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).Build();
				DataNode dn = cluster.GetDataNodes()[0];
				NUnit.Framework.Assert.AreEqual(1, dn.GetAllBpOs().Length);
				cluster.AddNameNode(conf, nnPort2);
				NUnit.Framework.Assert.AreEqual(2, dn.GetAllBpOs().Length);
				cluster.AddNameNode(conf, nnPort3);
				NUnit.Framework.Assert.AreEqual(3, dn.GetAllBpOs().Length);
				cluster.AddNameNode(conf, nnPort4);
				// Ensure a BPOfferService in the datanodes corresponds to
				// a namenode in the cluster
				ICollection<IPEndPoint> nnAddrsFromCluster = Sets.NewHashSet();
				for (int i = 0; i < 4; i++)
				{
					NUnit.Framework.Assert.IsTrue(nnAddrsFromCluster.AddItem(cluster.GetNameNode(i).GetNameNodeAddress
						()));
				}
				ICollection<IPEndPoint> nnAddrsFromDN = Sets.NewHashSet();
				foreach (BPOfferService bpos in dn.GetAllBpOs())
				{
					foreach (BPServiceActor bpsa in bpos.GetBPServiceActors())
					{
						NUnit.Framework.Assert.IsTrue(nnAddrsFromDN.AddItem(bpsa.GetNNSocketAddress()));
					}
				}
				NUnit.Framework.Assert.AreEqual(string.Empty, Joiner.On(",").Join(Sets.SymmetricDifference
					(nnAddrsFromCluster, nnAddrsFromDN)));
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
