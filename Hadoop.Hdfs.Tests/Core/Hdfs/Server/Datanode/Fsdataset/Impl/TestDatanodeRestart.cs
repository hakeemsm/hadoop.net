using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>Test if a datanode can correctly upgrade itself</summary>
	public class TestDatanodeRestart
	{
		// test finalized replicas persist across DataNode restarts
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFinalizedReplicas()
		{
			// bring up a cluster of 3
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1024L);
			conf.SetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, 512);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				// test finalized replicas
				string TopDir = "/test";
				DFSTestUtil util = new DFSTestUtil.Builder().SetName("TestDatanodeRestart").SetNumFiles
					(2).Build();
				util.CreateFiles(fs, TopDir, (short)3);
				util.WaitReplication(fs, TopDir, (short)3);
				util.CheckFiles(fs, TopDir);
				cluster.RestartDataNodes();
				cluster.WaitActive();
				util.CheckFiles(fs, TopDir);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		// test rbw replicas persist across DataNode restarts
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRbwReplicas()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1024L);
			conf.SetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, 512);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			cluster.WaitActive();
			try
			{
				TestRbwReplicas(cluster, false);
				TestRbwReplicas(cluster, true);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestRbwReplicas(MiniDFSCluster cluster, bool isCorrupt)
		{
			FSDataOutputStream @out = null;
			FileSystem fs = cluster.GetFileSystem();
			Path src = new Path("/test.txt");
			try
			{
				int fileLen = 515;
				// create some rbw replicas on disk
				byte[] writeBuf = new byte[fileLen];
				new Random().NextBytes(writeBuf);
				@out = fs.Create(src);
				@out.Write(writeBuf);
				@out.Hflush();
				DataNode dn = cluster.GetDataNodes()[0];
				foreach (FsVolumeSpi v in Dataset(dn).GetVolumes())
				{
					FsVolumeImpl volume = (FsVolumeImpl)v;
					FilePath currentDir = volume.GetCurrentDir().GetParentFile().GetParentFile();
					FilePath rbwDir = new FilePath(currentDir, "rbw");
					foreach (FilePath file in rbwDir.ListFiles())
					{
						if (isCorrupt && Block.IsBlockFilename(file))
						{
							new RandomAccessFile(file, "rw").SetLength(fileLen - 1);
						}
					}
				}
				// corrupt
				cluster.RestartDataNodes();
				cluster.WaitActive();
				dn = cluster.GetDataNodes()[0];
				// check volumeMap: one rwr replica
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				ReplicaMap replicas = Dataset(dn).volumeMap;
				NUnit.Framework.Assert.AreEqual(1, replicas.Size(bpid));
				ReplicaInfo replica = replicas.Replicas(bpid).GetEnumerator().Next();
				NUnit.Framework.Assert.AreEqual(HdfsServerConstants.ReplicaState.Rwr, replica.GetState
					());
				if (isCorrupt)
				{
					NUnit.Framework.Assert.AreEqual((fileLen - 1) / 512 * 512, replica.GetNumBytes());
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(fileLen, replica.GetNumBytes());
				}
				Dataset(dn).Invalidate(bpid, new Block[] { replica });
			}
			finally
			{
				IOUtils.CloseStream(@out);
				if (fs.Exists(src))
				{
					fs.Delete(src, false);
				}
				fs.Close();
			}
		}

		// test recovering unlinked tmp replicas
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoverReplicas()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 1024L);
			conf.SetInt(DFSConfigKeys.DfsClientWritePacketSizeKey, 512);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			cluster.WaitActive();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				for (int i = 0; i < 4; i++)
				{
					Path fileName = new Path("/test" + i);
					DFSTestUtil.CreateFile(fs, fileName, 1, (short)1, 0L);
					DFSTestUtil.WaitReplication(fs, fileName, (short)1);
				}
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				DataNode dn = cluster.GetDataNodes()[0];
				IEnumerator<ReplicaInfo> replicasItor = Dataset(dn).volumeMap.Replicas(bpid).GetEnumerator
					();
				ReplicaInfo replica = replicasItor.Next();
				CreateUnlinkTmpFile(replica, true, true);
				// rename block file
				CreateUnlinkTmpFile(replica, false, true);
				// rename meta file
				replica = replicasItor.Next();
				CreateUnlinkTmpFile(replica, true, false);
				// copy block file
				CreateUnlinkTmpFile(replica, false, false);
				// copy meta file
				replica = replicasItor.Next();
				CreateUnlinkTmpFile(replica, true, true);
				// rename block file
				CreateUnlinkTmpFile(replica, false, false);
				// copy meta file
				cluster.RestartDataNodes();
				cluster.WaitActive();
				dn = cluster.GetDataNodes()[0];
				// check volumeMap: 4 finalized replica
				ICollection<ReplicaInfo> replicas = Dataset(dn).volumeMap.Replicas(bpid);
				NUnit.Framework.Assert.AreEqual(4, replicas.Count);
				replicasItor = replicas.GetEnumerator();
				while (replicasItor.HasNext())
				{
					NUnit.Framework.Assert.AreEqual(HdfsServerConstants.ReplicaState.Finalized, replicasItor
						.Next().GetState());
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private static FsDatasetImpl Dataset(DataNode dn)
		{
			return (FsDatasetImpl)DataNodeTestUtils.GetFSDataset(dn);
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateUnlinkTmpFile(ReplicaInfo replicaInfo, bool changeBlockFile
			, bool isRename)
		{
			FilePath src;
			if (changeBlockFile)
			{
				src = replicaInfo.GetBlockFile();
			}
			else
			{
				src = replicaInfo.GetMetaFile();
			}
			FilePath dst = DatanodeUtil.GetUnlinkTmpFile(src);
			if (isRename)
			{
				src.RenameTo(dst);
			}
			else
			{
				FileInputStream @in = new FileInputStream(src);
				try
				{
					FileOutputStream @out = new FileOutputStream(dst);
					try
					{
						IOUtils.CopyBytes(@in, @out, 1);
					}
					finally
					{
						@out.Close();
					}
				}
				finally
				{
					@in.Close();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWaitForRegistrationOnRestart()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsDatanodeBpReadyTimeoutKey, 5);
			conf.SetInt(DFSConfigKeys.DfsClientSocketTimeoutKey, 5000);
			// This makes the datanode appear registered to the NN, but it won't be
			// able to get to the saved dn reg internally.
			DataNodeFaultInjector dnFaultInjector = new _DataNodeFaultInjector_224();
			DataNodeFaultInjector oldDnInjector = DataNodeFaultInjector.Get();
			DataNodeFaultInjector.Set(dnFaultInjector);
			MiniDFSCluster cluster = null;
			long start = 0;
			Path file = new Path("/reg");
			try
			{
				int numDNs = 1;
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDNs).Build();
				cluster.WaitActive();
				start = Runtime.CurrentTimeMillis();
				FileSystem fileSys = cluster.GetFileSystem();
				try
				{
					DFSTestUtil.CreateFile(fileSys, file, 10240L, (short)1, 0L);
					// It is a bug if this does not fail.
					throw new IOException("Did not fail!");
				}
				catch (RemoteException e)
				{
					long elapsed = Runtime.CurrentTimeMillis() - start;
					// timers have at-least semantics, so it should be at least 5 seconds.
					if (elapsed < 5000 || elapsed > 10000)
					{
						throw new IOException(elapsed + " seconds passed.", e);
					}
				}
				DataNodeFaultInjector.Set(oldDnInjector);
				// this should succeed now.
				DFSTestUtil.CreateFile(fileSys, file, 10240L, (short)1, 0L);
				// turn it back to under-construction, so that the client calls
				// getReplicaVisibleLength() rpc method against the datanode.
				fileSys.Append(file);
				// back to simulating unregistered node.
				DataNodeFaultInjector.Set(dnFaultInjector);
				byte[] buffer = new byte[8];
				start = Runtime.CurrentTimeMillis();
				try
				{
					fileSys.Open(file).Read(0L, buffer, 0, 1);
					throw new IOException("Did not fail!");
				}
				catch (IOException e)
				{
					long elapsed = Runtime.CurrentTimeMillis() - start;
					if (e.Message.Contains("readBlockLength"))
					{
						throw new IOException("Failed, but with unexpected exception:", e);
					}
					// timers have at-least semantics, so it should be at least 5 seconds.
					if (elapsed < 5000 || elapsed > 10000)
					{
						throw new IOException(elapsed + " seconds passed.", e);
					}
				}
				DataNodeFaultInjector.Set(oldDnInjector);
				fileSys.Open(file).Read(0L, buffer, 0, 1);
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

		private sealed class _DataNodeFaultInjector_224 : DataNodeFaultInjector
		{
			public _DataNodeFaultInjector_224()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void NoRegistration()
			{
				throw new IOException("no reg found for testing");
			}
		}
	}
}
