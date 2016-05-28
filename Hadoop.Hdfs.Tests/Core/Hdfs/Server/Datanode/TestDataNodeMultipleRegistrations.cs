using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	public class TestDataNodeMultipleRegistrations
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDataNodeMultipleRegistrations
			));

		internal Configuration conf;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new HdfsConfiguration();
		}

		/// <summary>
		/// start multiple NNs and single DN and verifies per BP registrations and
		/// handshakes.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Test2NNRegistration()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleFederatedTopology(2)).Build();
			try
			{
				cluster.WaitActive();
				NameNode nn1 = cluster.GetNameNode(0);
				NameNode nn2 = cluster.GetNameNode(1);
				NUnit.Framework.Assert.IsNotNull("cannot create nn1", nn1);
				NUnit.Framework.Assert.IsNotNull("cannot create nn2", nn2);
				string bpid1 = FSImageTestUtil.GetFSImage(nn1).GetBlockPoolID();
				string bpid2 = FSImageTestUtil.GetFSImage(nn2).GetBlockPoolID();
				string cid1 = FSImageTestUtil.GetFSImage(nn1).GetClusterID();
				string cid2 = FSImageTestUtil.GetFSImage(nn2).GetClusterID();
				int lv1 = FSImageTestUtil.GetFSImage(nn1).GetLayoutVersion();
				int lv2 = FSImageTestUtil.GetFSImage(nn2).GetLayoutVersion();
				int ns1 = FSImageTestUtil.GetFSImage(nn1).GetNamespaceID();
				int ns2 = FSImageTestUtil.GetFSImage(nn2).GetNamespaceID();
				NUnit.Framework.Assert.AreNotSame("namespace ids should be different", ns1, ns2);
				Log.Info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";bpid=" + bpid1 + ";uri=" + nn1.GetNameNodeAddress
					());
				Log.Info("nn2: lv=" + lv2 + ";cid=" + cid2 + ";bpid=" + bpid2 + ";uri=" + nn2.GetNameNodeAddress
					());
				// check number of volumes in fsdataset
				DataNode dn = cluster.GetDataNodes()[0];
				IDictionary<string, object> volInfos = dn.data.GetVolumeInfoMap();
				NUnit.Framework.Assert.IsTrue("No volumes in the fsdataset", volInfos.Count > 0);
				int i = 0;
				foreach (KeyValuePair<string, object> e in volInfos)
				{
					Log.Info("vol " + i++ + ") " + e.Key + ": " + e.Value);
				}
				// number of volumes should be 2 - [data1, data2]
				NUnit.Framework.Assert.AreEqual("number of volumes is wrong", 2, volInfos.Count);
				foreach (BPOfferService bpos in dn.GetAllBpOs())
				{
					Log.Info("BP: " + bpos);
				}
				BPOfferService bpos1 = dn.GetAllBpOs()[0];
				BPOfferService bpos2 = dn.GetAllBpOs()[1];
				// The order of bpos is not guaranteed, so fix the order
				if (GetNNSocketAddress(bpos1).Equals(nn2.GetNameNodeAddress()))
				{
					BPOfferService tmp = bpos1;
					bpos1 = bpos2;
					bpos2 = tmp;
				}
				NUnit.Framework.Assert.AreEqual("wrong nn address", GetNNSocketAddress(bpos1), nn1
					.GetNameNodeAddress());
				NUnit.Framework.Assert.AreEqual("wrong nn address", GetNNSocketAddress(bpos2), nn2
					.GetNameNodeAddress());
				NUnit.Framework.Assert.AreEqual("wrong bpid", bpos1.GetBlockPoolId(), bpid1);
				NUnit.Framework.Assert.AreEqual("wrong bpid", bpos2.GetBlockPoolId(), bpid2);
				NUnit.Framework.Assert.AreEqual("wrong cid", dn.GetClusterId(), cid1);
				NUnit.Framework.Assert.AreEqual("cid should be same", cid2, cid1);
				NUnit.Framework.Assert.AreEqual("namespace should be same", bpos1.bpNSInfo.namespaceID
					, ns1);
				NUnit.Framework.Assert.AreEqual("namespace should be same", bpos2.bpNSInfo.namespaceID
					, ns2);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private static IPEndPoint GetNNSocketAddress(BPOfferService bpos)
		{
			IList<BPServiceActor> actors = bpos.GetBPServiceActors();
			NUnit.Framework.Assert.AreEqual(1, actors.Count);
			return actors[0].GetNNSocketAddress();
		}

		/// <summary>starts single nn and single dn and verifies registration and handshake</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFedSingleNN()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NameNodePort(9927).Build
				();
			try
			{
				NameNode nn1 = cluster.GetNameNode();
				NUnit.Framework.Assert.IsNotNull("cannot create nn1", nn1);
				string bpid1 = FSImageTestUtil.GetFSImage(nn1).GetBlockPoolID();
				string cid1 = FSImageTestUtil.GetFSImage(nn1).GetClusterID();
				int lv1 = FSImageTestUtil.GetFSImage(nn1).GetLayoutVersion();
				Log.Info("nn1: lv=" + lv1 + ";cid=" + cid1 + ";bpid=" + bpid1 + ";uri=" + nn1.GetNameNodeAddress
					());
				// check number of vlumes in fsdataset
				DataNode dn = cluster.GetDataNodes()[0];
				IDictionary<string, object> volInfos = dn.data.GetVolumeInfoMap();
				NUnit.Framework.Assert.IsTrue("No volumes in the fsdataset", volInfos.Count > 0);
				int i = 0;
				foreach (KeyValuePair<string, object> e in volInfos)
				{
					Log.Info("vol " + i++ + ") " + e.Key + ": " + e.Value);
				}
				// number of volumes should be 2 - [data1, data2]
				NUnit.Framework.Assert.AreEqual("number of volumes is wrong", 2, volInfos.Count);
				foreach (BPOfferService bpos in dn.GetAllBpOs())
				{
					Log.Info("reg: bpid=" + "; name=" + bpos.bpRegistration + "; sid=" + bpos.bpRegistration
						.GetDatanodeUuid() + "; nna=" + GetNNSocketAddress(bpos));
				}
				// try block report
				BPOfferService bpos1 = dn.GetAllBpOs()[0];
				bpos1.TriggerBlockReportForTests();
				NUnit.Framework.Assert.AreEqual("wrong nn address", GetNNSocketAddress(bpos1), nn1
					.GetNameNodeAddress());
				NUnit.Framework.Assert.AreEqual("wrong bpid", bpos1.GetBlockPoolId(), bpid1);
				NUnit.Framework.Assert.AreEqual("wrong cid", dn.GetClusterId(), cid1);
				cluster.Shutdown();
				// Ensure all the BPOfferService threads are shutdown
				NUnit.Framework.Assert.AreEqual(0, dn.GetAllBpOs().Length);
				cluster = null;
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
		public virtual void TestClusterIdMismatch()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleFederatedTopology(2)).Build();
			try
			{
				cluster.WaitActive();
				DataNode dn = cluster.GetDataNodes()[0];
				BPOfferService[] bposs = dn.GetAllBpOs();
				Log.Info("dn bpos len (should be 2):" + bposs.Length);
				NUnit.Framework.Assert.AreEqual("should've registered with two namenodes", bposs.
					Length, 2);
				// add another namenode
				cluster.AddNameNode(conf, 9938);
				Sharpen.Thread.Sleep(500);
				// lets wait for the registration to happen
				bposs = dn.GetAllBpOs();
				Log.Info("dn bpos len (should be 3):" + bposs.Length);
				NUnit.Framework.Assert.AreEqual("should've registered with three namenodes", bposs
					.Length, 3);
				// change cluster id and another Namenode
				HdfsServerConstants.StartupOption.Format.SetClusterId("DifferentCID");
				cluster.AddNameNode(conf, 9948);
				NameNode nn4 = cluster.GetNameNode(3);
				NUnit.Framework.Assert.IsNotNull("cannot create nn4", nn4);
				Sharpen.Thread.Sleep(500);
				// lets wait for the registration to happen
				bposs = dn.GetAllBpOs();
				Log.Info("dn bpos len (still should be 3):" + bposs.Length);
				NUnit.Framework.Assert.AreEqual("should've registered with three namenodes", 3, bposs
					.Length);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestClusterIdMismatchAtStartupWithHA()
		{
			MiniDFSNNTopology top = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn0")).AddNN(new MiniDFSNNTopology.NNConf
				("nn1"))).AddNameservice(new MiniDFSNNTopology.NSConf("ns2").AddNN(new MiniDFSNNTopology.NNConf
				("nn2").SetClusterId("bad-cid")).AddNN(new MiniDFSNNTopology.NNConf("nn3").SetClusterId
				("bad-cid")));
			top.SetFederation(true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(top).NumDataNodes
				(0).Build();
			try
			{
				cluster.StartDataNodes(conf, 1, true, null, null);
				// let the initialization be complete
				Sharpen.Thread.Sleep(10000);
				DataNode dn = cluster.GetDataNodes()[0];
				NUnit.Framework.Assert.IsTrue("Datanode should be running", dn.IsDatanodeUp());
				NUnit.Framework.Assert.AreEqual("Only one BPOfferService should be running", 1, dn
					.GetAllBpOs().Length);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDNWithInvalidStorageWithHA()
		{
			MiniDFSNNTopology top = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn0").SetClusterId("cluster-1")).AddNN
				(new MiniDFSNNTopology.NNConf("nn1").SetClusterId("cluster-1")));
			top.SetFederation(true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(top).NumDataNodes
				(0).Build();
			try
			{
				cluster.StartDataNodes(conf, 1, true, null, null);
				// let the initialization be complete
				Sharpen.Thread.Sleep(10000);
				DataNode dn = cluster.GetDataNodes()[0];
				NUnit.Framework.Assert.IsTrue("Datanode should be running", dn.IsDatanodeUp());
				NUnit.Framework.Assert.AreEqual("BPOfferService should be running", 1, dn.GetAllBpOs
					().Length);
				MiniDFSCluster.DataNodeProperties dnProp = cluster.StopDataNode(0);
				cluster.GetNameNode(0).Stop();
				cluster.GetNameNode(1).Stop();
				Configuration nn1 = cluster.GetConfiguration(0);
				Configuration nn2 = cluster.GetConfiguration(1);
				// setting up invalid cluster
				HdfsServerConstants.StartupOption.Format.SetClusterId("cluster-2");
				DFSTestUtil.FormatNameNode(nn1);
				MiniDFSCluster.CopyNameDirs(FSNamesystem.GetNamespaceDirs(nn1), FSNamesystem.GetNamespaceDirs
					(nn2), nn2);
				cluster.RestartNameNode(0, false);
				cluster.RestartNameNode(1, false);
				cluster.RestartDataNode(dnProp);
				// let the initialization be complete
				Sharpen.Thread.Sleep(10000);
				dn = cluster.GetDataNodes()[0];
				NUnit.Framework.Assert.IsFalse("Datanode should have shutdown as only service failed"
					, dn.IsDatanodeUp());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestMiniDFSClusterWithMultipleNN()
		{
			Configuration conf = new HdfsConfiguration();
			// start Federated cluster and add a node.
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleFederatedTopology(2)).Build();
			// add a node
			try
			{
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual("(1)Should be 2 namenodes", 2, cluster.GetNumNameNodes
					());
				cluster.AddNameNode(conf, 0);
				NUnit.Framework.Assert.AreEqual("(1)Should be 3 namenodes", 3, cluster.GetNumNameNodes
					());
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.Fail("Failed to add NN to cluster:" + StringUtils.StringifyException
					(ioe));
			}
			finally
			{
				cluster.Shutdown();
			}
			// 2. start with Federation flag set
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
				(1)).Build();
			try
			{
				NUnit.Framework.Assert.IsNotNull(cluster);
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual("(2)Should be 1 namenodes", 1, cluster.GetNumNameNodes
					());
				// add a node
				cluster.AddNameNode(conf, 0);
				NUnit.Framework.Assert.AreEqual("(2)Should be 2 namenodes", 2, cluster.GetNumNameNodes
					());
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.Fail("Failed to add NN to cluster:" + StringUtils.StringifyException
					(ioe));
			}
			finally
			{
				cluster.Shutdown();
			}
			// 3. start non-federated
			conf = new HdfsConfiguration();
			cluster = new MiniDFSCluster.Builder(conf).Build();
			// add a node
			try
			{
				cluster.WaitActive();
				NUnit.Framework.Assert.IsNotNull(cluster);
				NUnit.Framework.Assert.AreEqual("(2)Should be 1 namenodes", 1, cluster.GetNumNameNodes
					());
				cluster.AddNameNode(conf, 9929);
				NUnit.Framework.Assert.Fail("shouldn't be able to add another NN to non federated cluster"
					);
			}
			catch (IOException e)
			{
				// correct 
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("cannot add namenode"));
				NUnit.Framework.Assert.AreEqual("(3)Should be 1 namenodes", 1, cluster.GetNumNameNodes
					());
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
