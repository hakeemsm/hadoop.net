using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Protobuf;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Invocation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>Unit tests for block tokens</summary>
	public class TestBlockToken
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestBlockToken));

		private const string Address = "0.0.0.0";

		static TestBlockToken()
		{
			((Log4JLogger)Client.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)Server.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslRpcClient.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslRpcServer.Log).GetLogger().SetLevel(Level.All);
			((Log4JLogger)SaslInputStream.Log).GetLogger().SetLevel(Level.All);
		}

		/// <summary>Directory where we can count our open file descriptors under Linux</summary>
		internal static readonly FilePath FdDir = new FilePath("/proc/self/fd/");

		internal readonly long blockKeyUpdateInterval = 10 * 60 * 1000;

		internal readonly long blockTokenLifetime = 2 * 60 * 1000;

		internal readonly ExtendedBlock block1 = new ExtendedBlock("0", 0L);

		internal readonly ExtendedBlock block2 = new ExtendedBlock("10", 10L);

		internal readonly ExtendedBlock block3 = new ExtendedBlock("-10", -108L);

		// 10 mins
		// 2 mins
		[SetUp]
		public virtual void DisableKerberos()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "simple");
			UserGroupInformation.SetConfiguration(conf);
		}

		private class GetLengthAnswer : Org.Mockito.Stubbing.Answer<ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto
			>
		{
			internal readonly BlockTokenSecretManager sm;

			internal readonly BlockTokenIdentifier ident;

			public GetLengthAnswer(BlockTokenSecretManager sm, BlockTokenIdentifier ident)
			{
				this.sm = sm;
				this.ident = ident;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto 
				Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				NUnit.Framework.Assert.AreEqual(2, args.Length);
				ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto req = (ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto
					)args[1];
				ICollection<TokenIdentifier> tokenIds = UserGroupInformation.GetCurrentUser().GetTokenIdentifiers
					();
				NUnit.Framework.Assert.AreEqual("Only one BlockTokenIdentifier expected", 1, tokenIds
					.Count);
				long result = 0;
				foreach (TokenIdentifier tokenId in tokenIds)
				{
					BlockTokenIdentifier id = (BlockTokenIdentifier)tokenId;
					Log.Info("Got: " + id.ToString());
					NUnit.Framework.Assert.IsTrue("Received BlockTokenIdentifier is wrong", ident.Equals
						(id));
					sm.CheckAccess(id, null, PBHelper.Convert(req.GetBlock()), BlockTokenSecretManager.AccessMode
						.Write);
					result = id.GetBlockId();
				}
				return ((ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto)ClientDatanodeProtocolProtos.GetReplicaVisibleLengthResponseProto
					.NewBuilder().SetLength(result).Build());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private BlockTokenIdentifier GenerateTokenId(BlockTokenSecretManager sm, ExtendedBlock
			 block, EnumSet<BlockTokenSecretManager.AccessMode> accessModes)
		{
			Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = sm.GenerateToken
				(block, accessModes);
			BlockTokenIdentifier id = sm.CreateIdentifier();
			id.ReadFields(new DataInputStream(new ByteArrayInputStream(token.GetIdentifier())
				));
			return id;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWritable()
		{
			TestWritable.TestWritable(new BlockTokenIdentifier());
			BlockTokenSecretManager sm = new BlockTokenSecretManager(blockKeyUpdateInterval, 
				blockTokenLifetime, 0, "fake-pool", null);
			TestWritable.TestWritable(GenerateTokenId(sm, block1, EnumSet.AllOf<BlockTokenSecretManager.AccessMode
				>()));
			TestWritable.TestWritable(GenerateTokenId(sm, block2, EnumSet.Of(BlockTokenSecretManager.AccessMode
				.Write)));
			TestWritable.TestWritable(GenerateTokenId(sm, block3, EnumSet.NoneOf<BlockTokenSecretManager.AccessMode
				>()));
		}

		/// <exception cref="System.Exception"/>
		private void TokenGenerationAndVerification(BlockTokenSecretManager master, BlockTokenSecretManager
			 slave)
		{
			// single-mode tokens
			foreach (BlockTokenSecretManager.AccessMode mode in BlockTokenSecretManager.AccessMode
				.Values())
			{
				// generated by master
				Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token1 = master.GenerateToken
					(block1, EnumSet.Of(mode));
				master.CheckAccess(token1, null, block1, mode);
				slave.CheckAccess(token1, null, block1, mode);
				// generated by slave
				Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token2 = slave.GenerateToken
					(block2, EnumSet.Of(mode));
				master.CheckAccess(token2, null, block2, mode);
				slave.CheckAccess(token2, null, block2, mode);
			}
			// multi-mode tokens
			Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> mtoken = master.GenerateToken
				(block3, EnumSet.AllOf<BlockTokenSecretManager.AccessMode>());
			foreach (BlockTokenSecretManager.AccessMode mode_1 in BlockTokenSecretManager.AccessMode
				.Values())
			{
				master.CheckAccess(mtoken, null, block3, mode_1);
				slave.CheckAccess(mtoken, null, block3, mode_1);
			}
		}

		/// <summary>test block key and token handling</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockTokenSecretManager()
		{
			BlockTokenSecretManager masterHandler = new BlockTokenSecretManager(blockKeyUpdateInterval
				, blockTokenLifetime, 0, "fake-pool", null);
			BlockTokenSecretManager slaveHandler = new BlockTokenSecretManager(blockKeyUpdateInterval
				, blockTokenLifetime, "fake-pool", null);
			ExportedBlockKeys keys = masterHandler.ExportKeys();
			slaveHandler.AddKeys(keys);
			TokenGenerationAndVerification(masterHandler, slaveHandler);
			// key updating
			masterHandler.UpdateKeys();
			TokenGenerationAndVerification(masterHandler, slaveHandler);
			keys = masterHandler.ExportKeys();
			slaveHandler.AddKeys(keys);
			TokenGenerationAndVerification(masterHandler, slaveHandler);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Com.Google.Protobuf.ServiceException"/>
		private static Server CreateMockDatanode(BlockTokenSecretManager sm, Org.Apache.Hadoop.Security.Token.Token
			<BlockTokenIdentifier> token, Configuration conf)
		{
			ClientDatanodeProtocolPB mockDN = Org.Mockito.Mockito.Mock<ClientDatanodeProtocolPB
				>();
			BlockTokenIdentifier id = sm.CreateIdentifier();
			id.ReadFields(new DataInputStream(new ByteArrayInputStream(token.GetIdentifier())
				));
			Org.Mockito.Mockito.DoAnswer(new TestBlockToken.GetLengthAnswer(sm, id)).When(mockDN
				).GetReplicaVisibleLength(Matchers.Any<RpcController>(), Matchers.Any<ClientDatanodeProtocolProtos.GetReplicaVisibleLengthRequestProto
				>());
			RPC.SetProtocolEngine(conf, typeof(ClientDatanodeProtocolPB), typeof(ProtobufRpcEngine
				));
			BlockingService service = ClientDatanodeProtocolProtos.ClientDatanodeProtocolService
				.NewReflectiveBlockingService(mockDN);
			return new RPC.Builder(conf).SetProtocol(typeof(ClientDatanodeProtocolPB)).SetInstance
				(service).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose(true).
				SetSecretManager(sm).Build();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockTokenRpc()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			BlockTokenSecretManager sm = new BlockTokenSecretManager(blockKeyUpdateInterval, 
				blockTokenLifetime, 0, "fake-pool", null);
			Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = sm.GenerateToken
				(block3, EnumSet.AllOf<BlockTokenSecretManager.AccessMode>());
			Server server = CreateMockDatanode(sm, token, conf);
			server.Start();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			UserGroupInformation ticket = UserGroupInformation.CreateRemoteUser(block3.ToString
				());
			ticket.AddToken(token);
			ClientDatanodeProtocol proxy = null;
			try
			{
				proxy = DFSUtil.CreateClientDatanodeProtocolProxy(addr, ticket, conf, NetUtils.GetDefaultSocketFactory
					(conf));
				NUnit.Framework.Assert.AreEqual(block3.GetBlockId(), proxy.GetReplicaVisibleLength
					(block3));
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		/// <summary>
		/// Test that fast repeated invocations of createClientDatanodeProtocolProxy
		/// will not end up using up thousands of sockets.
		/// </summary>
		/// <remarks>
		/// Test that fast repeated invocations of createClientDatanodeProtocolProxy
		/// will not end up using up thousands of sockets. This is a regression test
		/// for HDFS-1965.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockTokenRpcLeak()
		{
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			Assume.AssumeTrue(FdDir.Exists());
			BlockTokenSecretManager sm = new BlockTokenSecretManager(blockKeyUpdateInterval, 
				blockTokenLifetime, 0, "fake-pool", null);
			Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = sm.GenerateToken
				(block3, EnumSet.AllOf<BlockTokenSecretManager.AccessMode>());
			Server server = CreateMockDatanode(sm, token, conf);
			server.Start();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			DatanodeID fakeDnId = DFSTestUtil.GetLocalDatanodeID(addr.Port);
			ExtendedBlock b = new ExtendedBlock("fake-pool", new Org.Apache.Hadoop.Hdfs.Protocol.Block
				(12345L));
			LocatedBlock fakeBlock = new LocatedBlock(b, new DatanodeInfo[0]);
			fakeBlock.SetBlockToken(token);
			// Create another RPC proxy with the same configuration - this will never
			// attempt to connect anywhere -- but it causes the refcount on the
			// RPC "Client" object to stay above 0 such that RPC.stopProxy doesn't
			// actually close the TCP connections to the real target DN.
			ClientDatanodeProtocol proxyToNoWhere = RPC.GetProxy<ClientDatanodeProtocol>(ClientDatanodeProtocol
				.versionID, new IPEndPoint("1.1.1.1", 1), UserGroupInformation.CreateRemoteUser(
				"junk"), conf, NetUtils.GetDefaultSocketFactory(conf));
			ClientDatanodeProtocol proxy = null;
			int fdsAtStart = CountOpenFileDescriptors();
			try
			{
				long endTime = Time.Now() + 3000;
				while (Time.Now() < endTime)
				{
					proxy = DFSUtil.CreateClientDatanodeProtocolProxy(fakeDnId, conf, 1000, false, fakeBlock
						);
					NUnit.Framework.Assert.AreEqual(block3.GetBlockId(), proxy.GetReplicaVisibleLength
						(block3));
					if (proxy != null)
					{
						RPC.StopProxy(proxy);
					}
					Log.Info("Num open fds:" + CountOpenFileDescriptors());
				}
				int fdsAtEnd = CountOpenFileDescriptors();
				if (fdsAtEnd - fdsAtStart > 50)
				{
					NUnit.Framework.Assert.Fail("Leaked " + (fdsAtEnd - fdsAtStart) + " fds!");
				}
			}
			finally
			{
				server.Stop();
			}
			RPC.StopProxy(proxyToNoWhere);
		}

		/// <returns>the current number of file descriptors open by this process.</returns>
		private static int CountOpenFileDescriptors()
		{
			return FdDir.List().Length;
		}

		/// <summary>
		/// Test
		/// <see cref="BlockPoolTokenSecretManager"/>
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockPoolTokenSecretManager()
		{
			BlockPoolTokenSecretManager bpMgr = new BlockPoolTokenSecretManager();
			// Test BlockPoolSecretManager with upto 10 block pools
			for (int i = 0; i < 10; i++)
			{
				string bpid = Sharpen.Extensions.ToString(i);
				BlockTokenSecretManager masterHandler = new BlockTokenSecretManager(blockKeyUpdateInterval
					, blockTokenLifetime, 0, "fake-pool", null);
				BlockTokenSecretManager slaveHandler = new BlockTokenSecretManager(blockKeyUpdateInterval
					, blockTokenLifetime, "fake-pool", null);
				bpMgr.AddBlockPool(bpid, slaveHandler);
				ExportedBlockKeys keys = masterHandler.ExportKeys();
				bpMgr.AddKeys(bpid, keys);
				TokenGenerationAndVerification(masterHandler, bpMgr.Get(bpid));
				// Test key updating
				masterHandler.UpdateKeys();
				TokenGenerationAndVerification(masterHandler, bpMgr.Get(bpid));
				keys = masterHandler.ExportKeys();
				bpMgr.AddKeys(bpid, keys);
				TokenGenerationAndVerification(masterHandler, bpMgr.Get(bpid));
			}
		}

		/// <summary>
		/// This test writes a file and gets the block locations without closing the
		/// file, and tests the block token in the last block.
		/// </summary>
		/// <remarks>
		/// This test writes a file and gets the block locations without closing the
		/// file, and tests the block token in the last block. Block token is verified
		/// by ensuring it is of correct kind.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockTokenInLastLocatedBlock()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, 512);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				string fileName = "/testBlockTokenInLastLocatedBlock";
				Path filePath = new Path(fileName);
				FSDataOutputStream @out = fs.Create(filePath, (short)1);
				@out.Write(new byte[1000]);
				// ensure that the first block is written out (see FSOutputSummer#flush)
				@out.Flush();
				LocatedBlocks locatedBlocks = cluster.GetNameNodeRpc().GetBlockLocations(fileName
					, 0, 1000);
				while (locatedBlocks.GetLastLocatedBlock() == null)
				{
					Sharpen.Thread.Sleep(100);
					locatedBlocks = cluster.GetNameNodeRpc().GetBlockLocations(fileName, 0, 1000);
				}
				Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = locatedBlocks
					.GetLastLocatedBlock().GetBlockToken();
				NUnit.Framework.Assert.AreEqual(BlockTokenIdentifier.KindName, token.GetKind());
				@out.Close();
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
