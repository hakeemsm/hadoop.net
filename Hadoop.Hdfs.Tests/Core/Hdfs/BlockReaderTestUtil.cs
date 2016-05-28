using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Net;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Org.Apache.Hadoop.Net;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>A helper class to setup the cluster, and get to BlockReader and DataNode for a block.
	/// 	</summary>
	public class BlockReaderTestUtil
	{
		/// <summary>Returns true if we should run tests that generate large files (&gt; 1GB)
		/// 	</summary>
		public static bool ShouldTestLargeFiles()
		{
			string property = Runtime.GetProperty("hdfs.test.large.files");
			if (property == null)
			{
				return false;
			}
			if (property.IsEmpty())
			{
				return true;
			}
			return System.Boolean.Parse(property);
		}

		private HdfsConfiguration conf = null;

		private MiniDFSCluster cluster = null;

		/// <summary>Setup the cluster</summary>
		/// <exception cref="System.Exception"/>
		public BlockReaderTestUtil(int replicationFactor)
			: this(replicationFactor, new HdfsConfiguration())
		{
		}

		/// <exception cref="System.Exception"/>
		public BlockReaderTestUtil(int replicationFactor, HdfsConfiguration config)
		{
			this.conf = config;
			conf.SetInt(DFSConfigKeys.DfsReplicationKey, replicationFactor);
			cluster = new MiniDFSCluster.Builder(conf).Format(true).Build();
			cluster.WaitActive();
		}

		/// <summary>Shutdown cluster</summary>
		public virtual void Shutdown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		public virtual MiniDFSCluster GetCluster()
		{
			return cluster;
		}

		public virtual HdfsConfiguration GetConf()
		{
			return conf;
		}

		/// <summary>Create a file of the given size filled with random data.</summary>
		/// <returns>File data.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual byte[] WriteFile(Path filepath, int sizeKB)
		{
			FileSystem fs = cluster.GetFileSystem();
			// Write a file with the specified amount of data
			DataOutputStream os = fs.Create(filepath);
			byte[] data = new byte[1024 * sizeKB];
			new Random().NextBytes(data);
			os.Write(data);
			os.Close();
			return data;
		}

		/// <summary>Get the list of Blocks for a file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<LocatedBlock> GetFileBlocks(Path filepath, int sizeKB)
		{
			// Return the blocks we just wrote
			DFSClient dfsclient = GetDFSClient();
			return dfsclient.GetNamenode().GetBlockLocations(filepath.ToString(), 0, sizeKB *
				 1024).GetLocatedBlocks();
		}

		/// <summary>Get the DFSClient.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual DFSClient GetDFSClient()
		{
			IPEndPoint nnAddr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			return new DFSClient(nnAddr, conf);
		}

		/// <summary>Exercise the BlockReader and read length bytes.</summary>
		/// <remarks>
		/// Exercise the BlockReader and read length bytes.
		/// It does not verify the bytes read.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadAndCheckEOS(BlockReader reader, int length, bool expectEof
			)
		{
			byte[] buf = new byte[1024];
			int nRead = 0;
			while (nRead < length)
			{
				DFSClient.Log.Info("So far read " + nRead + " - going to read more.");
				int n = reader.Read(buf, 0, buf.Length);
				NUnit.Framework.Assert.IsTrue(n > 0);
				nRead += n;
			}
			if (expectEof)
			{
				DFSClient.Log.Info("Done reading, expect EOF for next read.");
				NUnit.Framework.Assert.AreEqual(-1, reader.Read(buf, 0, buf.Length));
			}
		}

		/// <summary>Get a BlockReader for the given block.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual BlockReader GetBlockReader(LocatedBlock testBlock, int offset, int
			 lenToRead)
		{
			return GetBlockReader(cluster, testBlock, offset, lenToRead);
		}

		/// <summary>Get a BlockReader for the given block.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static BlockReader GetBlockReader(MiniDFSCluster cluster, LocatedBlock testBlock
			, int offset, int lenToRead)
		{
			IPEndPoint targetAddr = null;
			ExtendedBlock block = testBlock.GetBlock();
			DatanodeInfo[] nodes = testBlock.GetLocations();
			targetAddr = NetUtils.CreateSocketAddr(nodes[0].GetXferAddr());
			DistributedFileSystem fs = cluster.GetFileSystem();
			return new BlockReaderFactory(fs.GetClient().GetConf()).SetInetSocketAddress(targetAddr
				).SetBlock(block).SetFileName(targetAddr.ToString() + ":" + block.GetBlockId()).
				SetBlockToken(testBlock.GetBlockToken()).SetStartOffset(offset).SetLength(lenToRead
				).SetVerifyChecksum(true).SetClientName("BlockReaderTestUtil").SetDatanodeInfo(nodes
				[0]).SetClientCacheContext(ClientContext.GetFromConf(fs.GetConf())).SetCachingStrategy
				(CachingStrategy.NewDefaultStrategy()).SetConfiguration(fs.GetConf()).SetAllowShortCircuitLocalReads
				(true).SetRemotePeerFactory(new _RemotePeerFactory_196(fs)).Build();
		}

		private sealed class _RemotePeerFactory_196 : RemotePeerFactory
		{
			public _RemotePeerFactory_196(DistributedFileSystem fs)
			{
				this.fs = fs;
			}

			/// <exception cref="System.IO.IOException"/>
			public Peer NewConnectedPeer(IPEndPoint addr, Org.Apache.Hadoop.Security.Token.Token
				<BlockTokenIdentifier> blockToken, DatanodeID datanodeId)
			{
				Peer peer = null;
				Socket sock = NetUtils.GetDefaultSocketFactory(fs.GetConf()).CreateSocket();
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
						IOUtils.CloseQuietly(sock);
					}
				}
				return peer;
			}

			private readonly DistributedFileSystem fs;
		}

		/// <summary>Get a DataNode that serves our testBlock.</summary>
		public virtual DataNode GetDataNode(LocatedBlock testBlock)
		{
			DatanodeInfo[] nodes = testBlock.GetLocations();
			int ipcport = nodes[0].GetIpcPort();
			return cluster.GetDataNode(ipcport);
		}

		public static void EnableHdfsCachingTracing()
		{
			LogManager.GetLogger(typeof(CacheReplicationMonitor).FullName).SetLevel(Level.Trace
				);
			LogManager.GetLogger(typeof(CacheManager).FullName).SetLevel(Level.Trace);
			LogManager.GetLogger(typeof(FsDatasetCache).FullName).SetLevel(Level.Trace);
		}

		public static void EnableBlockReaderFactoryTracing()
		{
			LogManager.GetLogger(typeof(BlockReaderFactory).FullName).SetLevel(Level.Trace);
			LogManager.GetLogger(typeof(ShortCircuitCache).FullName).SetLevel(Level.Trace);
			LogManager.GetLogger(typeof(ShortCircuitReplica).FullName).SetLevel(Level.Trace);
			LogManager.GetLogger(typeof(BlockReaderLocal).FullName).SetLevel(Level.Trace);
		}

		public static void EnableShortCircuitShmTracing()
		{
			LogManager.GetLogger(typeof(DfsClientShmManager).FullName).SetLevel(Level.Trace);
			LogManager.GetLogger(typeof(ShortCircuitRegistry).FullName).SetLevel(Level.Trace);
			LogManager.GetLogger(typeof(ShortCircuitShm).FullName).SetLevel(Level.Trace);
			LogManager.GetLogger(typeof(DataNode).FullName).SetLevel(Level.Trace);
		}
	}
}
