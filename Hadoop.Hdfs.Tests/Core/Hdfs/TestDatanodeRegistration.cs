using System;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests that a file need not be closed before its
	/// data can be read by another client.
	/// </summary>
	public class TestDatanodeRegistration
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestDatanodeRegistration
			));

		private class MonitorDNS : SecurityManager
		{
			internal int lookups = 0;

			public override void CheckPermission(Permission perm)
			{
			}

			public override void CheckConnect(string host, int port)
			{
				if (port == -1)
				{
					lookups++;
				}
			}
		}

		/// <summary>
		/// Ensure the datanode manager does not do host lookup after registration,
		/// especially for node reports.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDNSLookups()
		{
			TestDatanodeRegistration.MonitorDNS sm = new TestDatanodeRegistration.MonitorDNS(
				);
			Runtime.SetSecurityManager(sm);
			MiniDFSCluster cluster = null;
			try
			{
				HdfsConfiguration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(8).Build();
				cluster.WaitActive();
				int initialLookups = sm.lookups;
				NUnit.Framework.Assert.IsTrue("dns security manager is active", initialLookups !=
					 0);
				DatanodeManager dm = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
					();
				// make sure no lookups occur
				dm.RefreshNodes(conf);
				NUnit.Framework.Assert.AreEqual(initialLookups, sm.lookups);
				dm.RefreshNodes(conf);
				NUnit.Framework.Assert.AreEqual(initialLookups, sm.lookups);
				// ensure none of the reports trigger lookups
				dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType.All);
				NUnit.Framework.Assert.AreEqual(initialLookups, sm.lookups);
				dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType.Live);
				NUnit.Framework.Assert.AreEqual(initialLookups, sm.lookups);
				dm.GetDatanodeListForReport(HdfsConstants.DatanodeReportType.Dead);
				NUnit.Framework.Assert.AreEqual(initialLookups, sm.lookups);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				Runtime.SetSecurityManager(null);
			}
		}

		/// <summary>
		/// Regression test for HDFS-894 ensures that, when datanodes
		/// are restarted, the new IPC port is registered with the
		/// namenode.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeIpcPort()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
				DFSClient client = new DFSClient(addr, conf);
				// Restart datanodes
				cluster.RestartDataNodes();
				// Wait until we get a heartbeat from the new datanode
				DatanodeInfo[] report = client.DatanodeReport(HdfsConstants.DatanodeReportType.All
					);
				long firstUpdateAfterRestart = report[0].GetLastUpdate();
				bool gotHeartbeat = false;
				for (int i = 0; i < 10 && !gotHeartbeat; i++)
				{
					try
					{
						Sharpen.Thread.Sleep(i * 1000);
					}
					catch (Exception)
					{
					}
					report = client.DatanodeReport(HdfsConstants.DatanodeReportType.All);
					gotHeartbeat = (report[0].GetLastUpdate() > firstUpdateAfterRestart);
				}
				if (!gotHeartbeat)
				{
					NUnit.Framework.Assert.Fail("Never got a heartbeat from restarted datanode.");
				}
				int realIpcPort = cluster.GetDataNodes()[0].GetIpcPort();
				// Now make sure the reported IPC port is the correct one.
				NUnit.Framework.Assert.AreEqual(realIpcPort, report[0].GetIpcPort());
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
		public virtual void TestChangeStorageID()
		{
			string DnIpAddr = "127.0.0.1";
			string DnHostname = "localhost";
			int DnXferPort = 12345;
			int DnInfoPort = 12346;
			int DnInfoSecurePort = 12347;
			int DnIpcPort = 12348;
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
				DFSClient client = new DFSClient(addr, conf);
				NamenodeProtocols rpcServer = cluster.GetNameNodeRpc();
				// register a datanode
				DatanodeID dnId = new DatanodeID(DnIpAddr, DnHostname, "fake-datanode-id", DnXferPort
					, DnInfoPort, DnInfoSecurePort, DnIpcPort);
				long nnCTime = cluster.GetNamesystem().GetFSImage().GetStorage().GetCTime();
				StorageInfo mockStorageInfo = Org.Mockito.Mockito.Mock<StorageInfo>();
				Org.Mockito.Mockito.DoReturn(nnCTime).When(mockStorageInfo).GetCTime();
				Org.Mockito.Mockito.DoReturn(HdfsConstants.DatanodeLayoutVersion).When(mockStorageInfo
					).GetLayoutVersion();
				DatanodeRegistration dnReg = new DatanodeRegistration(dnId, mockStorageInfo, null
					, VersionInfo.GetVersion());
				rpcServer.RegisterDatanode(dnReg);
				DatanodeInfo[] report = client.DatanodeReport(HdfsConstants.DatanodeReportType.All
					);
				NUnit.Framework.Assert.AreEqual("Expected a registered datanode", 1, report.Length
					);
				// register the same datanode again with a different storage ID
				dnId = new DatanodeID(DnIpAddr, DnHostname, "changed-fake-datanode-id", DnXferPort
					, DnInfoPort, DnInfoSecurePort, DnIpcPort);
				dnReg = new DatanodeRegistration(dnId, mockStorageInfo, null, VersionInfo.GetVersion
					());
				rpcServer.RegisterDatanode(dnReg);
				report = client.DatanodeReport(HdfsConstants.DatanodeReportType.All);
				NUnit.Framework.Assert.AreEqual("Datanode with changed storage ID not recognized"
					, 1, report.Length);
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
		public virtual void TestRegistrationWithDifferentSoftwareVersions()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDatanodeMinSupportedNamenodeVersionKey, "3.0.0");
			conf.Set(DFSConfigKeys.DfsNamenodeMinSupportedDatanodeVersionKey, "3.0.0");
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				NamenodeProtocols rpcServer = cluster.GetNameNodeRpc();
				long nnCTime = cluster.GetNamesystem().GetFSImage().GetStorage().GetCTime();
				StorageInfo mockStorageInfo = Org.Mockito.Mockito.Mock<StorageInfo>();
				Org.Mockito.Mockito.DoReturn(nnCTime).When(mockStorageInfo).GetCTime();
				DatanodeRegistration mockDnReg = Org.Mockito.Mockito.Mock<DatanodeRegistration>();
				Org.Mockito.Mockito.DoReturn(HdfsConstants.DatanodeLayoutVersion).When(mockDnReg)
					.GetVersion();
				Org.Mockito.Mockito.DoReturn("127.0.0.1").When(mockDnReg).GetIpAddr();
				Org.Mockito.Mockito.DoReturn(123).When(mockDnReg).GetXferPort();
				Org.Mockito.Mockito.DoReturn("fake-storage-id").When(mockDnReg).GetDatanodeUuid();
				Org.Mockito.Mockito.DoReturn(mockStorageInfo).When(mockDnReg).GetStorageInfo();
				// Should succeed when software versions are the same.
				Org.Mockito.Mockito.DoReturn("3.0.0").When(mockDnReg).GetSoftwareVersion();
				rpcServer.RegisterDatanode(mockDnReg);
				// Should succeed when software version of DN is above minimum required by NN.
				Org.Mockito.Mockito.DoReturn("4.0.0").When(mockDnReg).GetSoftwareVersion();
				rpcServer.RegisterDatanode(mockDnReg);
				// Should fail when software version of DN is below minimum required by NN.
				Org.Mockito.Mockito.DoReturn("2.0.0").When(mockDnReg).GetSoftwareVersion();
				try
				{
					rpcServer.RegisterDatanode(mockDnReg);
					NUnit.Framework.Assert.Fail("Should not have been able to register DN with too-low version."
						);
				}
				catch (IncorrectVersionException ive)
				{
					GenericTestUtils.AssertExceptionContains("The reported DataNode version is too low"
						, ive);
					Log.Info("Got expected exception", ive);
				}
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
		public virtual void TestRegistrationWithDifferentSoftwareVersionsDuringUpgrade()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDatanodeMinSupportedNamenodeVersionKey, "1.0.0");
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Build();
				NamenodeProtocols rpcServer = cluster.GetNameNodeRpc();
				long nnCTime = cluster.GetNamesystem().GetFSImage().GetStorage().GetCTime();
				StorageInfo mockStorageInfo = Org.Mockito.Mockito.Mock<StorageInfo>();
				Org.Mockito.Mockito.DoReturn(nnCTime).When(mockStorageInfo).GetCTime();
				DatanodeRegistration mockDnReg = Org.Mockito.Mockito.Mock<DatanodeRegistration>();
				Org.Mockito.Mockito.DoReturn(HdfsConstants.DatanodeLayoutVersion).When(mockDnReg)
					.GetVersion();
				Org.Mockito.Mockito.DoReturn("fake-storage-id").When(mockDnReg).GetDatanodeUuid();
				Org.Mockito.Mockito.DoReturn(mockStorageInfo).When(mockDnReg).GetStorageInfo();
				// Should succeed when software versions are the same and CTimes are the
				// same.
				Org.Mockito.Mockito.DoReturn(VersionInfo.GetVersion()).When(mockDnReg).GetSoftwareVersion
					();
				Org.Mockito.Mockito.DoReturn("127.0.0.1").When(mockDnReg).GetIpAddr();
				Org.Mockito.Mockito.DoReturn(123).When(mockDnReg).GetXferPort();
				rpcServer.RegisterDatanode(mockDnReg);
				// Should succeed when software versions are the same and CTimes are
				// different.
				Org.Mockito.Mockito.DoReturn(nnCTime + 1).When(mockStorageInfo).GetCTime();
				rpcServer.RegisterDatanode(mockDnReg);
				// Should fail when software version of DN is different from NN and CTimes
				// are different.
				Org.Mockito.Mockito.DoReturn(VersionInfo.GetVersion() + ".1").When(mockDnReg).GetSoftwareVersion
					();
				try
				{
					rpcServer.RegisterDatanode(mockDnReg);
					NUnit.Framework.Assert.Fail("Should not have been able to register DN with different software"
						 + " versions and CTimes");
				}
				catch (IncorrectVersionException ive)
				{
					GenericTestUtils.AssertExceptionContains("does not match CTime of NN", ive);
					Log.Info("Got expected exception", ive);
				}
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
