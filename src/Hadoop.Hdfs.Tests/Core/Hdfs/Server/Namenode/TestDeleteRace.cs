using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Test race between delete and other operations.</summary>
	/// <remarks>
	/// Test race between delete and other operations.  For now only addBlock()
	/// is tested since all others are acquiring FSNamesystem lock for the
	/// whole duration.
	/// </remarks>
	public class TestDeleteRace
	{
		private const int BlockSize = 4096;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestDeleteRace));

		private static readonly Configuration conf = new HdfsConfiguration();

		private MiniDFSCluster cluster;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteAddBlockRace()
		{
			TestDeleteAddBlockRace(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteAddBlockRaceWithSnapshot()
		{
			TestDeleteAddBlockRace(true);
		}

		/// <exception cref="System.Exception"/>
		private void TestDeleteAddBlockRace(bool hasSnapshot)
		{
			try
			{
				conf.SetClass(DFSConfigKeys.DfsBlockReplicatorClassnameKey, typeof(TestDeleteRace.SlowBlockPlacementPolicy
					), typeof(BlockPlacementPolicy));
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = cluster.GetFileSystem();
				string fileName = "/testDeleteAddBlockRace";
				Path filePath = new Path(fileName);
				FSDataOutputStream @out = null;
				@out = fs.Create(filePath);
				if (hasSnapshot)
				{
					SnapshotTestHelper.CreateSnapshot((DistributedFileSystem)fs, new Path("/"), "s1");
				}
				Sharpen.Thread deleteThread = new TestDeleteRace.DeleteThread(this, fs, filePath);
				deleteThread.Start();
				try
				{
					// write data and syn to make sure a block is allocated.
					@out.Write(new byte[32], 0, 32);
					@out.Hsync();
					NUnit.Framework.Assert.Fail("Should have failed.");
				}
				catch (FileNotFoundException e)
				{
					GenericTestUtils.AssertExceptionContains(filePath.GetName(), e);
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

		private class SlowBlockPlacementPolicy : BlockPlacementPolicyDefault
		{
			public override DatanodeStorageInfo[] ChooseTarget(string srcPath, int numOfReplicas
				, Node writer, IList<DatanodeStorageInfo> chosenNodes, bool returnChosenNodes, ICollection
				<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy)
			{
				DatanodeStorageInfo[] results = base.ChooseTarget(srcPath, numOfReplicas, writer, 
					chosenNodes, returnChosenNodes, excludedNodes, blocksize, storagePolicy);
				try
				{
					Sharpen.Thread.Sleep(3000);
				}
				catch (Exception)
				{
				}
				return results;
			}
		}

		private class DeleteThread : Sharpen.Thread
		{
			private FileSystem fs;

			private Path path;

			internal DeleteThread(TestDeleteRace _enclosing, FileSystem fs, Path path)
			{
				this._enclosing = _enclosing;
				this.fs = fs;
				this.path = path;
			}

			public override void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
					TestDeleteRace.Log.Info("Deleting" + this.path);
					FSDirectory fsdir = this._enclosing.cluster.GetNamesystem().dir;
					INode fileINode = fsdir.GetINode4Write(this.path.ToString());
					INodeMap inodeMap = (INodeMap)Whitebox.GetInternalState(fsdir, "inodeMap");
					this.fs.Delete(this.path, false);
					// after deletion, add the inode back to the inodeMap
					inodeMap.Put(fileINode);
					TestDeleteRace.Log.Info("Deleted" + this.path);
				}
				catch (Exception e)
				{
					TestDeleteRace.Log.Info(e);
				}
			}

			private readonly TestDeleteRace _enclosing;
		}

		private class RenameThread : Sharpen.Thread
		{
			private FileSystem fs;

			private Path from;

			private Path to;

			internal RenameThread(TestDeleteRace _enclosing, FileSystem fs, Path from, Path to
				)
			{
				this._enclosing = _enclosing;
				this.fs = fs;
				this.from = from;
				this.to = to;
			}

			public override void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
					TestDeleteRace.Log.Info("Renaming " + this.from + " to " + this.to);
					this.fs.Rename(this.from, this.to);
					TestDeleteRace.Log.Info("Renamed " + this.from + " to " + this.to);
				}
				catch (Exception e)
				{
					TestDeleteRace.Log.Info(e);
				}
			}

			private readonly TestDeleteRace _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRenameRace()
		{
			try
			{
				conf.SetClass(DFSConfigKeys.DfsBlockReplicatorClassnameKey, typeof(TestDeleteRace.SlowBlockPlacementPolicy
					), typeof(BlockPlacementPolicy));
				cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fs = cluster.GetFileSystem();
				Path dirPath1 = new Path("/testRenameRace1");
				Path dirPath2 = new Path("/testRenameRace2");
				Path filePath = new Path("/testRenameRace1/file1");
				fs.Mkdirs(dirPath1);
				FSDataOutputStream @out = fs.Create(filePath);
				Sharpen.Thread renameThread = new TestDeleteRace.RenameThread(this, fs, dirPath1, 
					dirPath2);
				renameThread.Start();
				// write data and close to make sure a block is allocated.
				@out.Write(new byte[32], 0, 32);
				@out.Close();
				// Restart name node so that it replays edit. If old path was
				// logged in edit, it will fail to come up.
				cluster.RestartNameNode(0);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>Test race between delete operation and commitBlockSynchronization method.
		/// 	</summary>
		/// <remarks>
		/// Test race between delete operation and commitBlockSynchronization method.
		/// See HDFS-6825.
		/// </remarks>
		/// <param name="hasSnapshot"/>
		/// <exception cref="System.Exception"/>
		private void TestDeleteAndCommitBlockSynchronizationRace(bool hasSnapshot)
		{
			Log.Info("Start testing, hasSnapshot: " + hasSnapshot);
			AList<AbstractMap.SimpleImmutableEntry<string, bool>> testList = new AList<AbstractMap.SimpleImmutableEntry
				<string, bool>>();
			testList.AddItem(new AbstractMap.SimpleImmutableEntry<string, bool>("/test-file", 
				false));
			testList.AddItem(new AbstractMap.SimpleImmutableEntry<string, bool>("/test-file1"
				, true));
			testList.AddItem(new AbstractMap.SimpleImmutableEntry<string, bool>("/testdir/testdir1/test-file"
				, false));
			testList.AddItem(new AbstractMap.SimpleImmutableEntry<string, bool>("/testdir/testdir1/test-file1"
				, true));
			Path rootPath = new Path("/");
			Configuration conf = new Configuration();
			// Disable permissions so that another user can recover the lease.
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			FSDataOutputStream stm = null;
			IDictionary<DataNode, DatanodeProtocolClientSideTranslatorPB> dnMap = new Dictionary
				<DataNode, DatanodeProtocolClientSideTranslatorPB>();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				cluster.WaitActive();
				DistributedFileSystem fs = cluster.GetFileSystem();
				int stId = 0;
				foreach (AbstractMap.SimpleImmutableEntry<string, bool> stest in testList)
				{
					string testPath = stest.Key;
					bool mkSameDir = stest.Value;
					Log.Info("test on " + testPath + " mkSameDir: " + mkSameDir + " snapshot: " + hasSnapshot
						);
					Path fPath = new Path(testPath);
					//find grandest non-root parent
					Path grandestNonRootParent = fPath;
					while (!grandestNonRootParent.GetParent().Equals(rootPath))
					{
						grandestNonRootParent = grandestNonRootParent.GetParent();
					}
					stm = fs.Create(fPath);
					Log.Info("test on " + testPath + " created " + fPath);
					// write a half block
					AppendTestUtil.Write(stm, 0, BlockSize / 2);
					stm.Hflush();
					if (hasSnapshot)
					{
						SnapshotTestHelper.CreateSnapshot(fs, rootPath, "st" + stId.ToString());
						++stId;
					}
					// Look into the block manager on the active node for the block
					// under construction.
					NameNode nn = cluster.GetNameNode();
					ExtendedBlock blk = DFSTestUtil.GetFirstBlock(fs, fPath);
					DatanodeDescriptor expectedPrimary = DFSTestUtil.GetExpectedPrimaryNode(nn, blk);
					Log.Info("Expecting block recovery to be triggered on DN " + expectedPrimary);
					// Find the corresponding DN daemon, and spy on its connection to the
					// active.
					DataNode primaryDN = cluster.GetDataNode(expectedPrimary.GetIpcPort());
					DatanodeProtocolClientSideTranslatorPB nnSpy = dnMap[primaryDN];
					if (nnSpy == null)
					{
						nnSpy = DataNodeTestUtils.SpyOnBposToNN(primaryDN, nn);
						dnMap[primaryDN] = nnSpy;
					}
					// Delay the commitBlockSynchronization call
					GenericTestUtils.DelayAnswer delayer = new GenericTestUtils.DelayAnswer(Log);
					Org.Mockito.Mockito.DoAnswer(delayer).When(nnSpy).CommitBlockSynchronization(Org.Mockito.Mockito
						.Eq(blk), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito
						.Eq(true), Org.Mockito.Mockito.Eq(false), (DatanodeID[])Org.Mockito.Mockito.AnyObject
						(), (string[])Org.Mockito.Mockito.AnyObject());
					// new genstamp
					// new length
					// close file
					// delete block
					// new targets
					// new target storages
					fs.RecoverLease(fPath);
					Log.Info("Waiting for commitBlockSynchronization call from primary");
					delayer.WaitForCall();
					Log.Info("Deleting recursively " + grandestNonRootParent);
					fs.Delete(grandestNonRootParent, true);
					if (mkSameDir && !grandestNonRootParent.ToString().Equals(testPath))
					{
						Log.Info("Recreate dir " + grandestNonRootParent + " testpath: " + testPath);
						fs.Mkdirs(grandestNonRootParent);
					}
					delayer.Proceed();
					Log.Info("Now wait for result");
					delayer.WaitForResult();
					Exception t = delayer.GetThrown();
					if (t != null)
					{
						Log.Info("Result exception (snapshot: " + hasSnapshot + "): " + t);
					}
				}
				// end of loop each fPath
				Log.Info("Now check we can restart");
				cluster.RestartNameNodes();
				Log.Info("Restart finished");
			}
			finally
			{
				if (stm != null)
				{
					IOUtils.CloseStream(stm);
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteAndCommitBlockSynchonizationRaceNoSnapshot()
		{
			TestDeleteAndCommitBlockSynchronizationRace(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteAndCommitBlockSynchronizationRaceHasSnapshot()
		{
			TestDeleteAndCommitBlockSynchronizationRace(true);
		}
	}
}
