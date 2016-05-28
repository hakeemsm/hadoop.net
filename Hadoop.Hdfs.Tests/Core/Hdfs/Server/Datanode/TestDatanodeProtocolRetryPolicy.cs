using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>This tests DatanodeProtocol retry policy</summary>
	public class TestDatanodeProtocolRetryPolicy
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDatanodeProtocolRetryPolicy
			));

		private static readonly string DataDir = MiniDFSCluster.GetBaseDirectory() + "data";

		private DataNode dn;

		private Configuration conf;

		private bool tearDownDone;

		internal AList<StorageLocation> locations = new AList<StorageLocation>();

		private const string ClusterId = "testClusterID";

		private const string PoolId = "BP-TEST";

		private static readonly IPEndPoint NnAddr = new IPEndPoint("localhost", 5020);

		private static DatanodeRegistration datanodeRegistration = DFSTestUtil.GetLocalDatanodeRegistration
			();

		static TestDatanodeProtocolRetryPolicy()
		{
			((Log4JLogger)Log).GetLogger().SetLevel(Level.All);
		}

		/// <summary>Starts an instance of DataNode</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[SetUp]
		public virtual void StartUp()
		{
			tearDownDone = false;
			conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsDatanodeDataDirKey, DataDir);
			conf.Set(DFSConfigKeys.DfsDatanodeAddressKey, "0.0.0.0:0");
			conf.Set(DFSConfigKeys.DfsDatanodeHttpAddressKey, "0.0.0.0:0");
			conf.Set(DFSConfigKeys.DfsDatanodeIpcAddressKey, "0.0.0.0:0");
			conf.SetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesKey, 0);
			FileSystem.SetDefaultUri(conf, "hdfs://" + NnAddr.GetHostName() + ":" + NnAddr.Port
				);
			FilePath dataDir = new FilePath(DataDir);
			FileUtil.FullyDelete(dataDir);
			dataDir.Mkdirs();
			StorageLocation location = StorageLocation.Parse(dataDir.GetPath());
			locations.AddItem(location);
		}

		/// <summary>Cleans the resources and closes the instance of datanode</summary>
		/// <exception cref="System.IO.IOException">if an error occurred</exception>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (!tearDownDone && dn != null)
			{
				try
				{
					dn.Shutdown();
				}
				catch (Exception e)
				{
					Log.Error("Cannot close: ", e);
				}
				finally
				{
					FilePath dir = new FilePath(DataDir);
					if (dir.Exists())
					{
						NUnit.Framework.Assert.IsTrue("Cannot delete data-node dirs", FileUtil.FullyDelete
							(dir));
					}
				}
				tearDownDone = true;
			}
		}

		/// <exception cref="System.Exception"/>
		private void WaitForBlockReport(DatanodeProtocolClientSideTranslatorPB mockNN)
		{
			GenericTestUtils.WaitFor(new _Supplier_133(mockNN), 500, 100000);
		}

		private sealed class _Supplier_133 : Supplier<bool>
		{
			public _Supplier_133(DatanodeProtocolClientSideTranslatorPB mockNN)
			{
				this.mockNN = mockNN;
			}

			public bool Get()
			{
				try
				{
					Org.Mockito.Mockito.Verify(mockNN).BlockReport(Org.Mockito.Mockito.Eq(TestDatanodeProtocolRetryPolicy
						.datanodeRegistration), Org.Mockito.Mockito.Eq(TestDatanodeProtocolRetryPolicy.PoolId
						), Org.Mockito.Mockito.AnyObject<StorageBlockReport[]>(), Org.Mockito.Mockito.AnyObject
						<BlockReportContext>());
					return true;
				}
				catch (Exception t)
				{
					TestDatanodeProtocolRetryPolicy.Log.Info("waiting on block report: " + t.Message);
					return false;
				}
			}

			private readonly DatanodeProtocolClientSideTranslatorPB mockNN;
		}

		/// <summary>Verify the following scenario.</summary>
		/// <remarks>
		/// Verify the following scenario.
		/// 1. The initial DatanodeProtocol.registerDatanode succeeds.
		/// 2. DN starts heartbeat process.
		/// 3. In the first heartbeat, NN asks DN to reregister.
		/// 4. DN calls DatanodeProtocol.registerDatanode.
		/// 5. DatanodeProtocol.registerDatanode throws EOFException.
		/// 6. DN retries.
		/// 7. DatanodeProtocol.registerDatanode succeeds.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDatanodeRegistrationRetry()
		{
			DatanodeProtocolClientSideTranslatorPB namenode = Org.Mockito.Mockito.Mock<DatanodeProtocolClientSideTranslatorPB
				>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_166()).When(namenode).RegisterDatanode(Org.Mockito.Mockito
				.Any<DatanodeRegistration>());
			Org.Mockito.Mockito.When(namenode.VersionRequest()).ThenReturn(new NamespaceInfo(
				1, ClusterId, PoolId, 1L));
			Org.Mockito.Mockito.DoAnswer(new _Answer_190()).When(namenode).SendHeartbeat(Org.Mockito.Mockito
				.Any<DatanodeRegistration>(), Org.Mockito.Mockito.Any<StorageReport[]>(), Org.Mockito.Mockito
				.AnyLong(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito
				.AnyInt(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.Any<VolumeFailureSummary
				>());
			dn = new _DataNode_222(namenode, conf, locations, null);
			// Trigger a heartbeat so that it acknowledges the NN as active.
			dn.GetAllBpOs()[0].TriggerHeartbeatForTests();
			WaitForBlockReport(namenode);
		}

		private sealed class _Answer_166 : Answer<DatanodeRegistration>
		{
			public _Answer_166()
			{
				this.i = 0;
			}

			internal int i;

			/// <exception cref="System.Exception"/>
			public DatanodeRegistration Answer(InvocationOnMock invocation)
			{
				this.i++;
				if (this.i > 1 && this.i < 5)
				{
					TestDatanodeProtocolRetryPolicy.Log.Info("mockito exception " + this.i);
					throw new EOFException("TestDatanodeProtocolRetryPolicy");
				}
				else
				{
					DatanodeRegistration dr = (DatanodeRegistration)invocation.GetArguments()[0];
					TestDatanodeProtocolRetryPolicy.datanodeRegistration = new DatanodeRegistration(dr
						.GetDatanodeUuid(), dr);
					TestDatanodeProtocolRetryPolicy.Log.Info("mockito succeeded " + TestDatanodeProtocolRetryPolicy
						.datanodeRegistration);
					return TestDatanodeProtocolRetryPolicy.datanodeRegistration;
				}
			}
		}

		private sealed class _Answer_190 : Answer<HeartbeatResponse>
		{
			public _Answer_190()
			{
				this.i = 0;
			}

			internal int i;

			/// <exception cref="System.Exception"/>
			public HeartbeatResponse Answer(InvocationOnMock invocation)
			{
				this.i++;
				HeartbeatResponse heartbeatResponse;
				if (this.i == 1)
				{
					TestDatanodeProtocolRetryPolicy.Log.Info("mockito heartbeatResponse registration "
						 + this.i);
					heartbeatResponse = new HeartbeatResponse(new DatanodeCommand[] { RegisterCommand
						.Register }, new NNHAStatusHeartbeat(HAServiceProtocol.HAServiceState.Active, 1)
						, null);
				}
				else
				{
					TestDatanodeProtocolRetryPolicy.Log.Info("mockito heartbeatResponse " + this.i);
					heartbeatResponse = new HeartbeatResponse(new DatanodeCommand[0], new NNHAStatusHeartbeat
						(HAServiceProtocol.HAServiceState.Active, 1), null);
				}
				return heartbeatResponse;
			}
		}

		private sealed class _DataNode_222 : DataNode
		{
			public _DataNode_222(DatanodeProtocolClientSideTranslatorPB namenode, Configuration
				 baseArg1, IList<StorageLocation> baseArg2, SecureDataNodeStarter.SecureResources
				 baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
				this.namenode = namenode;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override DatanodeProtocolClientSideTranslatorPB ConnectToNN(IPEndPoint nnAddr
				)
			{
				NUnit.Framework.Assert.AreEqual(TestDatanodeProtocolRetryPolicy.NnAddr, nnAddr);
				return namenode;
			}

			private readonly DatanodeProtocolClientSideTranslatorPB namenode;
		}
	}
}
