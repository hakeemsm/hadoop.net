using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Proto;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test transferring RBW between datanodes</summary>
	public class TestTransferRbw
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Datanode.TestTransferRbw
			));

		private static readonly Random Ran = new Random();

		private const short Replication = (short)1;

		/// <exception cref="System.Exception"/>
		private static ReplicaBeingWritten GetRbw(DataNode datanode, string bpid)
		{
			return (ReplicaBeingWritten)GetReplica(datanode, bpid, HdfsServerConstants.ReplicaState
				.Rbw);
		}

		/// <exception cref="System.Exception"/>
		private static ReplicaInPipeline GetReplica(DataNode datanode, string bpid, HdfsServerConstants.ReplicaState
			 expectedState)
		{
			ICollection<ReplicaInfo> replicas = FsDatasetTestUtil.GetReplicas(datanode.GetFSDataset
				(), bpid);
			for (int i = 0; i < 5 && replicas.Count == 0; i++)
			{
				Log.Info("wait since replicas.size() == 0; i=" + i);
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.AreEqual(1, replicas.Count);
			ReplicaInfo r = replicas.GetEnumerator().Next();
			NUnit.Framework.Assert.AreEqual(expectedState, r.GetState());
			return (ReplicaInPipeline)r;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTransferRbw()
		{
			HdfsConfiguration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication
				).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem fs = cluster.GetFileSystem();
				//create a file, write some data and leave it open. 
				Path p = new Path("/foo");
				int size = (1 << 16) + Ran.Next(1 << 16);
				Log.Info("size = " + size);
				FSDataOutputStream @out = fs.Create(p, Replication);
				byte[] bytes = new byte[1024];
				for (int remaining = size; remaining > 0; )
				{
					Ran.NextBytes(bytes);
					int len = bytes.Length < remaining ? bytes.Length : remaining;
					@out.Write(bytes, 0, len);
					@out.Hflush();
					remaining -= len;
				}
				//get the RBW
				ReplicaBeingWritten oldrbw;
				DataNode newnode;
				DatanodeInfo newnodeinfo;
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				{
					DataNode oldnode = cluster.GetDataNodes()[0];
					oldrbw = GetRbw(oldnode, bpid);
					Log.Info("oldrbw = " + oldrbw);
					//add a datanode
					cluster.StartDataNodes(conf, 1, true, null, null);
					newnode = cluster.GetDataNodes()[Replication];
					DatanodeInfo oldnodeinfo;
					{
						DatanodeInfo[] datatnodeinfos = cluster.GetNameNodeRpc().GetDatanodeReport(HdfsConstants.DatanodeReportType
							.Live);
						NUnit.Framework.Assert.AreEqual(2, datatnodeinfos.Length);
						int i = 0;
						for (DatanodeRegistration dnReg = newnode.GetDNRegistrationForBP(bpid); i < datatnodeinfos
							.Length && !datatnodeinfos[i].Equals(dnReg); i++)
						{
						}
						NUnit.Framework.Assert.IsTrue(i < datatnodeinfos.Length);
						newnodeinfo = datatnodeinfos[i];
						oldnodeinfo = datatnodeinfos[1 - i];
					}
					//transfer RBW
					ExtendedBlock b = new ExtendedBlock(bpid, oldrbw.GetBlockId(), oldrbw.GetBytesAcked
						(), oldrbw.GetGenerationStamp());
					DataTransferProtos.BlockOpResponseProto s = DFSTestUtil.TransferRbw(b, DFSClientAdapter
						.GetDFSClient(fs), oldnodeinfo, newnodeinfo);
					NUnit.Framework.Assert.AreEqual(DataTransferProtos.Status.Success, s.GetStatus());
				}
				//check new rbw
				ReplicaBeingWritten newrbw = GetRbw(newnode, bpid);
				Log.Info("newrbw = " + newrbw);
				NUnit.Framework.Assert.AreEqual(oldrbw.GetBlockId(), newrbw.GetBlockId());
				NUnit.Framework.Assert.AreEqual(oldrbw.GetGenerationStamp(), newrbw.GetGenerationStamp
					());
				NUnit.Framework.Assert.AreEqual(oldrbw.GetVisibleLength(), newrbw.GetVisibleLength
					());
				Log.Info("DONE");
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		public TestTransferRbw()
		{
			{
				((Log4JLogger)DataNode.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
