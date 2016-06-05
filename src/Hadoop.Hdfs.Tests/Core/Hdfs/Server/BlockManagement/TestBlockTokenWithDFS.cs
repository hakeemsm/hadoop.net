using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using NUnit.Framework;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Balancer;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestBlockTokenWithDFS
	{
		private const int BlockSize = 1024;

		private const int FileSize = 2 * BlockSize;

		private const string FileToRead = "/fileToRead.dat";

		private const string FileToWrite = "/fileToWrite.dat";

		private const string FileToAppend = "/fileToAppend.dat";

		private readonly byte[] rawData = new byte[FileSize];

		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(FileSystem fs, Path filename)
		{
			FSDataOutputStream @out = fs.Create(filename);
			@out.Write(rawData);
			@out.Close();
		}

		// read a file using blockSeekTo()
		private bool CheckFile1(FSDataInputStream @in)
		{
			byte[] toRead = new byte[FileSize];
			int totalRead = 0;
			int nRead = 0;
			try
			{
				while ((nRead = @in.Read(toRead, totalRead, toRead.Length - totalRead)) > 0)
				{
					totalRead += nRead;
				}
			}
			catch (IOException)
			{
				return false;
			}
			NUnit.Framework.Assert.AreEqual("Cannot read file.", toRead.Length, totalRead);
			return CheckFile(toRead);
		}

		// read a file using fetchBlockByteRange()
		private bool CheckFile2(FSDataInputStream @in)
		{
			byte[] toRead = new byte[FileSize];
			try
			{
				NUnit.Framework.Assert.AreEqual("Cannot read file", toRead.Length, @in.Read(0, toRead
					, 0, toRead.Length));
			}
			catch (IOException)
			{
				return false;
			}
			return CheckFile(toRead);
		}

		private bool CheckFile(byte[] fileToCheck)
		{
			if (fileToCheck.Length != rawData.Length)
			{
				return false;
			}
			for (int i = 0; i < fileToCheck.Length; i++)
			{
				if (fileToCheck[i] != rawData[i])
				{
					return false;
				}
			}
			return true;
		}

		// creates a file and returns a descriptor for writing to it
		/// <exception cref="System.IO.IOException"/>
		private static FSDataOutputStream WriteFile(FileSystem fileSys, Path name, short 
			repl, long blockSize)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), repl, blockSize);
			return stm;
		}

		// try reading a block using a BlockReader directly
		private static void TryRead(Configuration conf, LocatedBlock lblock, bool shouldSucceed
			)
		{
			IPEndPoint targetAddr = null;
			IOException ioe = null;
			BlockReader blockReader = null;
			ExtendedBlock block = lblock.GetBlock();
			try
			{
				DatanodeInfo[] nodes = lblock.GetLocations();
				targetAddr = NetUtils.CreateSocketAddr(nodes[0].GetXferAddr());
				blockReader = new BlockReaderFactory(new DFSClient.Conf(conf)).SetFileName(BlockReaderFactory
					.GetFileName(targetAddr, "test-blockpoolid", block.GetBlockId())).SetBlock(block
					).SetBlockToken(lblock.GetBlockToken()).SetInetSocketAddress(targetAddr).SetStartOffset
					(0).SetLength(-1).SetVerifyChecksum(true).SetClientName("TestBlockTokenWithDFS")
					.SetDatanodeInfo(nodes[0]).SetCachingStrategy(CachingStrategy.NewDefaultStrategy
					()).SetClientCacheContext(ClientContext.GetFromConf(conf)).SetConfiguration(conf
					).SetRemotePeerFactory(new _RemotePeerFactory_162(conf)).Build();
			}
			catch (IOException ex)
			{
				ioe = ex;
			}
			finally
			{
				if (blockReader != null)
				{
					try
					{
						blockReader.Close();
					}
					catch (IOException e)
					{
						throw new RuntimeException(e);
					}
				}
			}
			if (shouldSucceed)
			{
				NUnit.Framework.Assert.IsNotNull("OP_READ_BLOCK: access token is invalid, " + "when it is expected to be valid"
					, blockReader);
			}
			else
			{
				NUnit.Framework.Assert.IsNotNull("OP_READ_BLOCK: access token is valid, " + "when it is expected to be invalid"
					, ioe);
				NUnit.Framework.Assert.IsTrue("OP_READ_BLOCK failed due to reasons other than access token: "
					, ioe is InvalidBlockTokenException);
			}
		}

		private sealed class _RemotePeerFactory_162 : RemotePeerFactory
		{
			public _RemotePeerFactory_162(Configuration conf)
			{
				this.conf = conf;
			}

			/// <exception cref="System.IO.IOException"/>
			public Peer NewConnectedPeer(IPEndPoint addr, Org.Apache.Hadoop.Security.Token.Token
				<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
			{
				Peer peer = null;
				Socket sock = NetUtils.GetDefaultSocketFactory(conf).CreateSocket();
				try
				{
					sock.Connect(addr, HdfsServerConstants.ReadTimeout);
					sock.ReceiveTimeout = HdfsServerConstants.ReadTimeout;
					peer = TcpPeerServer.PeerFromSocket(sock);
				}
				finally
				{
					if (peer == null)
					{
						IOUtils.CloseSocket(sock);
					}
				}
				return peer;
			}

			private readonly Configuration conf;
		}

		// get a conf for testing
		private static Configuration GetConf(int numDataNodes)
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt("io.bytes.per.checksum", BlockSize);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, numDataNodes);
			conf.SetInt("ipc.client.connect.max.retries", 0);
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			return conf;
		}

		/// <summary>
		/// testing that APPEND operation can handle token expiration when
		/// re-establishing pipeline is needed
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppend()
		{
			MiniDFSCluster cluster = null;
			int numDataNodes = 2;
			Configuration conf = GetConf(numDataNodes);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual(numDataNodes, cluster.GetDataNodes().Count);
				NameNode nn = cluster.GetNameNode();
				BlockManager bm = nn.GetNamesystem().GetBlockManager();
				BlockTokenSecretManager sm = bm.GetBlockTokenSecretManager();
				// set a short token lifetime (1 second)
				SecurityTestUtil.SetBlockTokenLifetime(sm, 1000L);
				Path fileToAppend = new Path(FileToAppend);
				FileSystem fs = cluster.GetFileSystem();
				// write a one-byte file
				FSDataOutputStream stm = WriteFile(fs, fileToAppend, (short)numDataNodes, BlockSize
					);
				stm.Write(rawData, 0, 1);
				stm.Close();
				// open the file again for append
				stm = fs.Append(fileToAppend);
				int mid = rawData.Length - 1;
				stm.Write(rawData, 1, mid - 1);
				stm.Hflush();
				/*
				* wait till token used in stm expires
				*/
				Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = DFSTestUtil.
					GetBlockToken(stm);
				while (!SecurityTestUtil.IsBlockTokenExpired(token))
				{
					try
					{
						Sharpen.Thread.Sleep(10);
					}
					catch (Exception)
					{
					}
				}
				// remove a datanode to force re-establishing pipeline
				cluster.StopDataNode(0);
				// append the rest of the file
				stm.Write(rawData, mid, rawData.Length - mid);
				stm.Close();
				// check if append is successful
				FSDataInputStream in5 = fs.Open(fileToAppend);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in5));
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
		/// testing that WRITE operation can handle token expiration when
		/// re-establishing pipeline is needed
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWrite()
		{
			MiniDFSCluster cluster = null;
			int numDataNodes = 2;
			Configuration conf = GetConf(numDataNodes);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual(numDataNodes, cluster.GetDataNodes().Count);
				NameNode nn = cluster.GetNameNode();
				BlockManager bm = nn.GetNamesystem().GetBlockManager();
				BlockTokenSecretManager sm = bm.GetBlockTokenSecretManager();
				// set a short token lifetime (1 second)
				SecurityTestUtil.SetBlockTokenLifetime(sm, 1000L);
				Path fileToWrite = new Path(FileToWrite);
				FileSystem fs = cluster.GetFileSystem();
				FSDataOutputStream stm = WriteFile(fs, fileToWrite, (short)numDataNodes, BlockSize
					);
				// write a partial block
				int mid = rawData.Length - 1;
				stm.Write(rawData, 0, mid);
				stm.Hflush();
				/*
				* wait till token used in stm expires
				*/
				Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> token = DFSTestUtil.
					GetBlockToken(stm);
				while (!SecurityTestUtil.IsBlockTokenExpired(token))
				{
					try
					{
						Sharpen.Thread.Sleep(10);
					}
					catch (Exception)
					{
					}
				}
				// remove a datanode to force re-establishing pipeline
				cluster.StopDataNode(0);
				// write the rest of the file
				stm.Write(rawData, mid, rawData.Length - mid);
				stm.Close();
				// check if write is successful
				FSDataInputStream in4 = fs.Open(fileToWrite);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in4));
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
		public virtual void TestRead()
		{
			MiniDFSCluster cluster = null;
			int numDataNodes = 2;
			Configuration conf = GetConf(numDataNodes);
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual(numDataNodes, cluster.GetDataNodes().Count);
				NameNode nn = cluster.GetNameNode();
				NamenodeProtocols nnProto = nn.GetRpcServer();
				BlockManager bm = nn.GetNamesystem().GetBlockManager();
				BlockTokenSecretManager sm = bm.GetBlockTokenSecretManager();
				// set a short token lifetime (1 second) initially
				SecurityTestUtil.SetBlockTokenLifetime(sm, 1000L);
				Path fileToRead = new Path(FileToRead);
				FileSystem fs = cluster.GetFileSystem();
				CreateFile(fs, fileToRead);
				/*
				* setup for testing expiration handling of cached tokens
				*/
				// read using blockSeekTo(). Acquired tokens are cached in in1
				FSDataInputStream in1 = fs.Open(fileToRead);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in1));
				// read using blockSeekTo(). Acquired tokens are cached in in2
				FSDataInputStream in2 = fs.Open(fileToRead);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in2));
				// read using fetchBlockByteRange(). Acquired tokens are cached in in3
				FSDataInputStream in3 = fs.Open(fileToRead);
				NUnit.Framework.Assert.IsTrue(CheckFile2(in3));
				/*
				* testing READ interface on DN using a BlockReader
				*/
				DFSClient client = null;
				try
				{
					client = new DFSClient(new IPEndPoint("localhost", cluster.GetNameNodePort()), conf
						);
				}
				finally
				{
					if (client != null)
					{
						client.Close();
					}
				}
				IList<LocatedBlock> locatedBlocks = nnProto.GetBlockLocations(FileToRead, 0, FileSize
					).GetLocatedBlocks();
				LocatedBlock lblock = locatedBlocks[0];
				// first block
				Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> myToken = lblock.GetBlockToken
					();
				// verify token is not expired
				NUnit.Framework.Assert.IsFalse(SecurityTestUtil.IsBlockTokenExpired(myToken));
				// read with valid token, should succeed
				TryRead(conf, lblock, true);
				/*
				* wait till myToken and all cached tokens in in1, in2 and in3 expire
				*/
				while (!SecurityTestUtil.IsBlockTokenExpired(myToken))
				{
					try
					{
						Sharpen.Thread.Sleep(10);
					}
					catch (Exception)
					{
					}
				}
				/*
				* continue testing READ interface on DN using a BlockReader
				*/
				// verify token is expired
				NUnit.Framework.Assert.IsTrue(SecurityTestUtil.IsBlockTokenExpired(myToken));
				// read should fail
				TryRead(conf, lblock, false);
				// use a valid new token
				lblock.SetBlockToken(sm.GenerateToken(lblock.GetBlock(), EnumSet.Of(BlockTokenSecretManager.AccessMode
					.Read)));
				// read should succeed
				TryRead(conf, lblock, true);
				// use a token with wrong blockID
				ExtendedBlock wrongBlock = new ExtendedBlock(lblock.GetBlock().GetBlockPoolId(), 
					lblock.GetBlock().GetBlockId() + 1);
				lblock.SetBlockToken(sm.GenerateToken(wrongBlock, EnumSet.Of(BlockTokenSecretManager.AccessMode
					.Read)));
				// read should fail
				TryRead(conf, lblock, false);
				// use a token with wrong access modes
				lblock.SetBlockToken(sm.GenerateToken(lblock.GetBlock(), EnumSet.Of(BlockTokenSecretManager.AccessMode
					.Write, BlockTokenSecretManager.AccessMode.Copy, BlockTokenSecretManager.AccessMode
					.Replace)));
				// read should fail
				TryRead(conf, lblock, false);
				// set a long token lifetime for future tokens
				SecurityTestUtil.SetBlockTokenLifetime(sm, 600 * 1000L);
				/*
				* testing that when cached tokens are expired, DFSClient will re-fetch
				* tokens transparently for READ.
				*/
				// confirm all tokens cached in in1 are expired by now
				IList<LocatedBlock> lblocks = DFSTestUtil.GetAllBlocks(in1);
				foreach (LocatedBlock blk in lblocks)
				{
					NUnit.Framework.Assert.IsTrue(SecurityTestUtil.IsBlockTokenExpired(blk.GetBlockToken
						()));
				}
				// verify blockSeekTo() is able to re-fetch token transparently
				in1.Seek(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in1));
				// confirm all tokens cached in in2 are expired by now
				IList<LocatedBlock> lblocks2 = DFSTestUtil.GetAllBlocks(in2);
				foreach (LocatedBlock blk_1 in lblocks2)
				{
					NUnit.Framework.Assert.IsTrue(SecurityTestUtil.IsBlockTokenExpired(blk_1.GetBlockToken
						()));
				}
				// verify blockSeekTo() is able to re-fetch token transparently (testing
				// via another interface method)
				NUnit.Framework.Assert.IsTrue(in2.SeekToNewSource(0));
				NUnit.Framework.Assert.IsTrue(CheckFile1(in2));
				// confirm all tokens cached in in3 are expired by now
				IList<LocatedBlock> lblocks3 = DFSTestUtil.GetAllBlocks(in3);
				foreach (LocatedBlock blk_2 in lblocks3)
				{
					NUnit.Framework.Assert.IsTrue(SecurityTestUtil.IsBlockTokenExpired(blk_2.GetBlockToken
						()));
				}
				// verify fetchBlockByteRange() is able to re-fetch token transparently
				NUnit.Framework.Assert.IsTrue(CheckFile2(in3));
				/*
				* testing that after datanodes are restarted on the same ports, cached
				* tokens should still work and there is no need to fetch new tokens from
				* namenode. This test should run while namenode is down (to make sure no
				* new tokens can be fetched from namenode).
				*/
				// restart datanodes on the same ports that they currently use
				NUnit.Framework.Assert.IsTrue(cluster.RestartDataNodes(true));
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual(numDataNodes, cluster.GetDataNodes().Count);
				cluster.ShutdownNameNode(0);
				// confirm tokens cached in in1 are still valid
				lblocks = DFSTestUtil.GetAllBlocks(in1);
				foreach (LocatedBlock blk_3 in lblocks)
				{
					NUnit.Framework.Assert.IsFalse(SecurityTestUtil.IsBlockTokenExpired(blk_3.GetBlockToken
						()));
				}
				// verify blockSeekTo() still works (forced to use cached tokens)
				in1.Seek(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in1));
				// confirm tokens cached in in2 are still valid
				lblocks2 = DFSTestUtil.GetAllBlocks(in2);
				foreach (LocatedBlock blk_4 in lblocks2)
				{
					NUnit.Framework.Assert.IsFalse(SecurityTestUtil.IsBlockTokenExpired(blk_4.GetBlockToken
						()));
				}
				// verify blockSeekTo() still works (forced to use cached tokens)
				in2.SeekToNewSource(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in2));
				// confirm tokens cached in in3 are still valid
				lblocks3 = DFSTestUtil.GetAllBlocks(in3);
				foreach (LocatedBlock blk_5 in lblocks3)
				{
					NUnit.Framework.Assert.IsFalse(SecurityTestUtil.IsBlockTokenExpired(blk_5.GetBlockToken
						()));
				}
				// verify fetchBlockByteRange() still works (forced to use cached tokens)
				NUnit.Framework.Assert.IsTrue(CheckFile2(in3));
				/*
				* testing that when namenode is restarted, cached tokens should still
				* work and there is no need to fetch new tokens from namenode. Like the
				* previous test, this test should also run while namenode is down. The
				* setup for this test depends on the previous test.
				*/
				// restart the namenode and then shut it down for test
				cluster.RestartNameNode(0);
				cluster.ShutdownNameNode(0);
				// verify blockSeekTo() still works (forced to use cached tokens)
				in1.Seek(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in1));
				// verify again blockSeekTo() still works (forced to use cached tokens)
				in2.SeekToNewSource(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in2));
				// verify fetchBlockByteRange() still works (forced to use cached tokens)
				NUnit.Framework.Assert.IsTrue(CheckFile2(in3));
				/*
				* testing that after both namenode and datanodes got restarted (namenode
				* first, followed by datanodes), DFSClient can't access DN without
				* re-fetching tokens and is able to re-fetch tokens transparently. The
				* setup of this test depends on the previous test.
				*/
				// restore the cluster and restart the datanodes for test
				cluster.RestartNameNode(0);
				NUnit.Framework.Assert.IsTrue(cluster.RestartDataNodes(true));
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual(numDataNodes, cluster.GetDataNodes().Count);
				// shutdown namenode so that DFSClient can't get new tokens from namenode
				cluster.ShutdownNameNode(0);
				// verify blockSeekTo() fails (cached tokens become invalid)
				in1.Seek(0);
				NUnit.Framework.Assert.IsFalse(CheckFile1(in1));
				// verify fetchBlockByteRange() fails (cached tokens become invalid)
				NUnit.Framework.Assert.IsFalse(CheckFile2(in3));
				// restart the namenode to allow DFSClient to re-fetch tokens
				cluster.RestartNameNode(0);
				// verify blockSeekTo() works again (by transparently re-fetching
				// tokens from namenode)
				in1.Seek(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in1));
				in2.SeekToNewSource(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in2));
				// verify fetchBlockByteRange() works again (by transparently
				// re-fetching tokens from namenode)
				NUnit.Framework.Assert.IsTrue(CheckFile2(in3));
				/*
				* testing that when datanodes are restarted on different ports, DFSClient
				* is able to re-fetch tokens transparently to connect to them
				*/
				// restart datanodes on newly assigned ports
				NUnit.Framework.Assert.IsTrue(cluster.RestartDataNodes(false));
				cluster.WaitActive();
				NUnit.Framework.Assert.AreEqual(numDataNodes, cluster.GetDataNodes().Count);
				// verify blockSeekTo() is able to re-fetch token transparently
				in1.Seek(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in1));
				// verify blockSeekTo() is able to re-fetch token transparently
				in2.SeekToNewSource(0);
				NUnit.Framework.Assert.IsTrue(CheckFile1(in2));
				// verify fetchBlockByteRange() is able to re-fetch token transparently
				NUnit.Framework.Assert.IsTrue(CheckFile2(in3));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Integration testing of access token, involving NN, DN, and Balancer</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEnd2End()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsBlockAccessTokenEnableKey, true);
			new TestBalancer().IntegrationTest(conf);
		}

		public TestBlockTokenWithDFS()
		{
			{
				((Log4JLogger)DFSClient.Log).GetLogger().SetLevel(Level.All);
				Random r = new Random();
				r.NextBytes(rawData);
			}
		}
	}
}
