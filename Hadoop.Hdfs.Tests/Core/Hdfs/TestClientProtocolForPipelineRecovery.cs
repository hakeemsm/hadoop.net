using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This tests pipeline recovery related client protocol works correct or not.
	/// 	</summary>
	public class TestClientProtocolForPipelineRecovery
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNewStamp()
		{
			int numDataNodes = 1;
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes
				).Build();
			try
			{
				cluster.WaitActive();
				FileSystem fileSys = cluster.GetFileSystem();
				NamenodeProtocols namenode = cluster.GetNameNodeRpc();
				/* Test writing to finalized replicas */
				Path file = new Path("dataprotocol.dat");
				DFSTestUtil.CreateFile(fileSys, file, 1L, (short)numDataNodes, 0L);
				// get the first blockid for the file
				ExtendedBlock firstBlock = DFSTestUtil.GetFirstBlock(fileSys, file);
				// test getNewStampAndToken on a finalized block
				try
				{
					namenode.UpdateBlockForPipeline(firstBlock, string.Empty);
					NUnit.Framework.Assert.Fail("Can not get a new GS from a finalized block");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.Contains("is not under Construction"));
				}
				// test getNewStampAndToken on a non-existent block
				try
				{
					long newBlockId = firstBlock.GetBlockId() + 1;
					ExtendedBlock newBlock = new ExtendedBlock(firstBlock.GetBlockPoolId(), newBlockId
						, 0, firstBlock.GetGenerationStamp());
					namenode.UpdateBlockForPipeline(newBlock, string.Empty);
					NUnit.Framework.Assert.Fail("Cannot get a new GS from a non-existent block");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.Contains("does not exist"));
				}
				/* Test RBW replicas */
				// change first block to a RBW
				DFSOutputStream @out = null;
				try
				{
					@out = (DFSOutputStream)(fileSys.Append(file).GetWrappedStream());
					@out.Write(1);
					@out.Hflush();
					FSDataInputStream @in = null;
					try
					{
						@in = fileSys.Open(file);
						firstBlock = DFSTestUtil.GetAllBlocks(@in)[0].GetBlock();
					}
					finally
					{
						IOUtils.CloseStream(@in);
					}
					// test non-lease holder
					DFSClient dfs = ((DistributedFileSystem)fileSys).dfs;
					try
					{
						namenode.UpdateBlockForPipeline(firstBlock, "test" + dfs.clientName);
						NUnit.Framework.Assert.Fail("Cannot get a new GS for a non lease holder");
					}
					catch (LeaseExpiredException e)
					{
						NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Lease mismatch"));
					}
					// test null lease holder
					try
					{
						namenode.UpdateBlockForPipeline(firstBlock, null);
						NUnit.Framework.Assert.Fail("Cannot get a new GS for a null lease holder");
					}
					catch (LeaseExpiredException e)
					{
						NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Lease mismatch"));
					}
					// test getNewStampAndToken on a rbw block
					namenode.UpdateBlockForPipeline(firstBlock, dfs.clientName);
				}
				finally
				{
					IOUtils.CloseStream(@out);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test whether corrupt replicas are detected correctly during pipeline
		/// recoveries.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestPipelineRecoveryForLastBlock()
		{
			DFSClientFaultInjector faultInjector = Org.Mockito.Mockito.Mock<DFSClientFaultInjector
				>();
			DFSClientFaultInjector oldInjector = DFSClientFaultInjector.instance;
			DFSClientFaultInjector.instance = faultInjector;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsClientBlockWriteLocatefollowingblockRetriesKey, 3);
			MiniDFSCluster cluster = null;
			try
			{
				int numDataNodes = 3;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				FileSystem fileSys = cluster.GetFileSystem();
				Path file = new Path("dataprotocol1.dat");
				Org.Mockito.Mockito.When(faultInjector.FailPacket()).ThenReturn(true);
				DFSTestUtil.CreateFile(fileSys, file, 68000000L, (short)numDataNodes, 0L);
				// At this point, NN should have accepted only valid replicas.
				// Read should succeed.
				FSDataInputStream @in = fileSys.Open(file);
				try
				{
					int c = @in.Read();
				}
				catch (BlockMissingException)
				{
					// Test will fail with BlockMissingException if NN does not update the
					// replica state based on the latest report.
					NUnit.Framework.Assert.Fail("Block is missing because the file was closed with" +
						 " corrupt replicas.");
				}
			}
			finally
			{
				DFSClientFaultInjector.instance = oldInjector;
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPacketTransmissionDelay()
		{
			// Make the first datanode to not relay heartbeat packet.
			DataNodeFaultInjector dnFaultInjector = new _DataNodeFaultInjector_171();
			DataNodeFaultInjector oldDnInjector = DataNodeFaultInjector.Get();
			DataNodeFaultInjector.Set(dnFaultInjector);
			// Setting the timeout to be 3 seconds. Normally heartbeat packet
			// would be sent every 1.5 seconds if there is no data traffic.
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsClientSocketTimeoutKey, "3000");
			MiniDFSCluster cluster = null;
			try
			{
				int numDataNodes = 2;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				FileSystem fs = cluster.GetFileSystem();
				FSDataOutputStream @out = fs.Create(new Path("noheartbeat.dat"), (short)2);
				@out.Write(unchecked((int)(0x31)));
				@out.Hflush();
				DFSOutputStream dfsOut = (DFSOutputStream)@out.GetWrappedStream();
				// original pipeline
				DatanodeInfo[] orgNodes = dfsOut.GetPipeline();
				// Cause the second datanode to timeout on reading packet
				Sharpen.Thread.Sleep(3500);
				@out.Write(unchecked((int)(0x32)));
				@out.Hflush();
				// new pipeline
				DatanodeInfo[] newNodes = dfsOut.GetPipeline();
				@out.Close();
				bool contains = false;
				for (int i = 0; i < newNodes.Length; i++)
				{
					if (orgNodes[0].GetXferAddr().Equals(newNodes[i].GetXferAddr()))
					{
						throw new IOException("The first datanode should have been replaced.");
					}
					if (orgNodes[1].GetXferAddr().Equals(newNodes[i].GetXferAddr()))
					{
						contains = true;
					}
				}
				NUnit.Framework.Assert.IsTrue(contains);
			}
			finally
			{
				DataNodeFaultInjector.Set(oldDnInjector);
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _DataNodeFaultInjector_171 : DataNodeFaultInjector
		{
			public _DataNodeFaultInjector_171()
			{
			}

			public override bool DropHeartbeatPacket()
			{
				return true;
			}
		}

		/// <summary>Test recovery on restart OOB message.</summary>
		/// <remarks>
		/// Test recovery on restart OOB message. It also tests the delivery of
		/// OOB ack originating from the primary datanode. Since there is only
		/// one node in the cluster, failure of restart-recovery will fail the
		/// test.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPipelineRecoveryOnOOB()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsClientDatanodeRestartTimeoutKey, "15");
			MiniDFSCluster cluster = null;
			try
			{
				int numDataNodes = 1;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				FileSystem fileSys = cluster.GetFileSystem();
				Path file = new Path("dataprotocol2.dat");
				DFSTestUtil.CreateFile(fileSys, file, 10240L, (short)1, 0L);
				DFSOutputStream @out = (DFSOutputStream)(fileSys.Append(file).GetWrappedStream());
				@out.Write(1);
				@out.Hflush();
				DFSAdmin dfsadmin = new DFSAdmin(conf);
				DataNode dn = cluster.GetDataNodes()[0];
				string dnAddr = dn.GetDatanodeId().GetIpcAddr(false);
				// issue shutdown to the datanode.
				string[] args1 = new string[] { "-shutdownDatanode", dnAddr, "upgrade" };
				NUnit.Framework.Assert.AreEqual(0, dfsadmin.Run(args1));
				// Wait long enough to receive an OOB ack before closing the file.
				Sharpen.Thread.Sleep(4000);
				// Retart the datanode 
				cluster.RestartDataNode(0, true);
				// The following forces a data packet and end of block packets to be sent. 
				@out.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test restart timeout</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPipelineRecoveryOnRestartFailure()
		{
			Configuration conf = new HdfsConfiguration();
			conf.Set(DFSConfigKeys.DfsClientDatanodeRestartTimeoutKey, "5");
			MiniDFSCluster cluster = null;
			try
			{
				int numDataNodes = 2;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDataNodes).Build();
				cluster.WaitActive();
				FileSystem fileSys = cluster.GetFileSystem();
				Path file = new Path("dataprotocol3.dat");
				DFSTestUtil.CreateFile(fileSys, file, 10240L, (short)2, 0L);
				DFSOutputStream @out = (DFSOutputStream)(fileSys.Append(file).GetWrappedStream());
				@out.Write(1);
				@out.Hflush();
				DFSAdmin dfsadmin = new DFSAdmin(conf);
				DataNode dn = cluster.GetDataNodes()[0];
				string dnAddr1 = dn.GetDatanodeId().GetIpcAddr(false);
				// issue shutdown to the datanode.
				string[] args1 = new string[] { "-shutdownDatanode", dnAddr1, "upgrade" };
				NUnit.Framework.Assert.AreEqual(0, dfsadmin.Run(args1));
				Sharpen.Thread.Sleep(4000);
				// This should succeed without restarting the node. The restart will
				// expire and regular pipeline recovery will kick in. 
				@out.Close();
				// At this point there is only one node in the cluster. 
				@out = (DFSOutputStream)(fileSys.Append(file).GetWrappedStream());
				@out.Write(1);
				@out.Hflush();
				dn = cluster.GetDataNodes()[1];
				string dnAddr2 = dn.GetDatanodeId().GetIpcAddr(false);
				// issue shutdown to the datanode.
				string[] args2 = new string[] { "-shutdownDatanode", dnAddr2, "upgrade" };
				NUnit.Framework.Assert.AreEqual(0, dfsadmin.Run(args2));
				Sharpen.Thread.Sleep(4000);
				try
				{
					// close should fail
					@out.Close();
					System.Diagnostics.Debug.Assert(false);
				}
				catch (IOException)
				{
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
