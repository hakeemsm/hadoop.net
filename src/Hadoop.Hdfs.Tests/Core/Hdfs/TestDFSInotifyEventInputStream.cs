using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Inotify;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSInotifyEventInputStream
	{
		private const int BlockSize = 1024;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestDFSInotifyEventInputStream
			));

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException"/>
		public static EventBatch WaitForNextEvents(DFSInotifyEventInputStream eis)
		{
			EventBatch batch = null;
			while ((batch = eis.Poll()) == null)
			{
			}
			return batch;
		}

		private static long CheckTxid(EventBatch batch, long prevTxid)
		{
			NUnit.Framework.Assert.IsTrue("Previous txid " + prevTxid + " was not less than "
				 + "new txid " + batch.GetTxid(), prevTxid < batch.GetTxid());
			return batch.GetTxid();
		}

		/// <summary>
		/// If this test fails, check whether the newly added op should map to an
		/// inotify event, and if so, establish the mapping in
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.InotifyFSEditLogOpTranslator"/>
		/// and update testBasic() to include the new op.
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestOpcodeCount()
		{
			NUnit.Framework.Assert.AreEqual(50, FSEditLogOpCodes.Values().Length);
		}

		/// <summary>Tests all FsEditLogOps that are converted to inotify events.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException"/>
		public virtual void TestBasic()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			// so that we can get an atime change
			conf.SetLong(DFSConfigKeys.DfsNamenodeAccesstimePrecisionKey, 1);
			MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
			builder.GetDfsBuilder().NumDataNodes(2);
			MiniQJMHACluster cluster = builder.Build();
			try
			{
				cluster.GetDfsCluster().WaitActive();
				cluster.GetDfsCluster().TransitionToActive(0);
				DFSClient client = new DFSClient(cluster.GetDfsCluster().GetNameNode(0).GetNameNodeAddress
					(), conf);
				FileSystem fs = cluster.GetDfsCluster().GetFileSystem(0);
				DFSTestUtil.CreateFile(fs, new Path("/file"), BlockSize, (short)1, 0L);
				DFSTestUtil.CreateFile(fs, new Path("/file3"), BlockSize, (short)1, 0L);
				DFSTestUtil.CreateFile(fs, new Path("/file5"), BlockSize, (short)1, 0L);
				DFSInotifyEventInputStream eis = client.GetInotifyEventStream();
				client.Rename("/file", "/file4", null);
				// RenameOp -> RenameEvent
				client.Rename("/file4", "/file2");
				// RenameOldOp -> RenameEvent
				// DeleteOp, AddOp -> UnlinkEvent, CreateEvent
				OutputStream os = client.Create("/file2", true, (short)2, BlockSize);
				os.Write(new byte[BlockSize]);
				os.Close();
				// CloseOp -> CloseEvent
				// AddOp -> AppendEvent
				os = client.Append("/file2", BlockSize, EnumSet.Of(CreateFlag.Append), null, null
					);
				os.Write(new byte[BlockSize]);
				os.Close();
				// CloseOp -> CloseEvent
				Sharpen.Thread.Sleep(10);
				// so that the atime will get updated on the next line
				client.Open("/file2").Read(new byte[1]);
				// TimesOp -> MetadataUpdateEvent
				// SetReplicationOp -> MetadataUpdateEvent
				client.SetReplication("/file2", (short)1);
				// ConcatDeleteOp -> AppendEvent, UnlinkEvent, CloseEvent
				client.Concat("/file2", new string[] { "/file3" });
				client.Delete("/file2", false);
				// DeleteOp -> UnlinkEvent
				client.Mkdirs("/dir", null, false);
				// MkdirOp -> CreateEvent
				// SetPermissionsOp -> MetadataUpdateEvent
				client.SetPermission("/dir", FsPermission.ValueOf("-rw-rw-rw-"));
				// SetOwnerOp -> MetadataUpdateEvent
				client.SetOwner("/dir", "username", "groupname");
				client.CreateSymlink("/dir", "/dir2", false);
				// SymlinkOp -> CreateEvent
				client.SetXAttr("/file5", "user.field", Sharpen.Runtime.GetBytesForString("value"
					), EnumSet.Of(XAttrSetFlag.Create));
				// SetXAttrOp -> MetadataUpdateEvent
				// RemoveXAttrOp -> MetadataUpdateEvent
				client.RemoveXAttr("/file5", "user.field");
				// SetAclOp -> MetadataUpdateEvent
				client.SetAcl("/file5", AclEntry.ParseAclSpec("user::rwx,user:foo:rw-,group::r--,other::---"
					, true));
				client.RemoveAcl("/file5");
				// SetAclOp -> MetadataUpdateEvent
				client.Rename("/file5", "/dir");
				// RenameOldOp -> RenameEvent
				EventBatch batch = null;
				// RenameOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				long txid = batch.GetTxid();
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Rename);
				Event.RenameEvent re = (Event.RenameEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.AreEqual("/file4", re.GetDstPath());
				NUnit.Framework.Assert.AreEqual("/file", re.GetSrcPath());
				NUnit.Framework.Assert.IsTrue(re.GetTimestamp() > 0);
				long eventsBehind = eis.GetTxidsBehindEstimate();
				// RenameOldOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Rename);
				Event.RenameEvent re2 = (Event.RenameEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(re2.GetDstPath().Equals("/file2"));
				NUnit.Framework.Assert.IsTrue(re2.GetSrcPath().Equals("/file4"));
				NUnit.Framework.Assert.IsTrue(re.GetTimestamp() > 0);
				// AddOp with overwrite
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Create);
				Event.CreateEvent ce = (Event.CreateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(ce.GetiNodeType() == Event.CreateEvent.INodeType.File
					);
				NUnit.Framework.Assert.IsTrue(ce.GetPath().Equals("/file2"));
				NUnit.Framework.Assert.IsTrue(ce.GetCtime() > 0);
				NUnit.Framework.Assert.IsTrue(ce.GetReplication() > 0);
				NUnit.Framework.Assert.IsTrue(ce.GetSymlinkTarget() == null);
				NUnit.Framework.Assert.IsTrue(ce.GetOverwrite());
				NUnit.Framework.Assert.AreEqual(BlockSize, ce.GetDefaultBlockSize());
				// CloseOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Close);
				Event.CloseEvent ce2 = (Event.CloseEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(ce2.GetPath().Equals("/file2"));
				NUnit.Framework.Assert.IsTrue(ce2.GetFileSize() > 0);
				NUnit.Framework.Assert.IsTrue(ce2.GetTimestamp() > 0);
				// AppendOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Append);
				Event.AppendEvent append2 = (Event.AppendEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.AreEqual("/file2", append2.GetPath());
				NUnit.Framework.Assert.IsFalse(append2.ToNewBlock());
				// CloseOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Close);
				NUnit.Framework.Assert.IsTrue(((Event.CloseEvent)batch.GetEvents()[0]).GetPath().
					Equals("/file2"));
				// TimesOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Metadata);
				Event.MetadataUpdateEvent mue = (Event.MetadataUpdateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(mue.GetPath().Equals("/file2"));
				NUnit.Framework.Assert.IsTrue(mue.GetMetadataType() == Event.MetadataUpdateEvent.MetadataType
					.Times);
				// SetReplicationOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Metadata);
				Event.MetadataUpdateEvent mue2 = (Event.MetadataUpdateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(mue2.GetPath().Equals("/file2"));
				NUnit.Framework.Assert.IsTrue(mue2.GetMetadataType() == Event.MetadataUpdateEvent.MetadataType
					.Replication);
				NUnit.Framework.Assert.IsTrue(mue2.GetReplication() == 1);
				// ConcatDeleteOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(3, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Append);
				NUnit.Framework.Assert.IsTrue(((Event.AppendEvent)batch.GetEvents()[0]).GetPath()
					.Equals("/file2"));
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[1].GetEventType() == Event.EventType
					.Unlink);
				Event.UnlinkEvent ue2 = (Event.UnlinkEvent)batch.GetEvents()[1];
				NUnit.Framework.Assert.IsTrue(ue2.GetPath().Equals("/file3"));
				NUnit.Framework.Assert.IsTrue(ue2.GetTimestamp() > 0);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[2].GetEventType() == Event.EventType
					.Close);
				Event.CloseEvent ce3 = (Event.CloseEvent)batch.GetEvents()[2];
				NUnit.Framework.Assert.IsTrue(ce3.GetPath().Equals("/file2"));
				NUnit.Framework.Assert.IsTrue(ce3.GetTimestamp() > 0);
				// DeleteOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Unlink);
				Event.UnlinkEvent ue = (Event.UnlinkEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(ue.GetPath().Equals("/file2"));
				NUnit.Framework.Assert.IsTrue(ue.GetTimestamp() > 0);
				// MkdirOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Create);
				Event.CreateEvent ce4 = (Event.CreateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(ce4.GetiNodeType() == Event.CreateEvent.INodeType.Directory
					);
				NUnit.Framework.Assert.IsTrue(ce4.GetPath().Equals("/dir"));
				NUnit.Framework.Assert.IsTrue(ce4.GetCtime() > 0);
				NUnit.Framework.Assert.IsTrue(ce4.GetReplication() == 0);
				NUnit.Framework.Assert.IsTrue(ce4.GetSymlinkTarget() == null);
				// SetPermissionsOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Metadata);
				Event.MetadataUpdateEvent mue3 = (Event.MetadataUpdateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(mue3.GetPath().Equals("/dir"));
				NUnit.Framework.Assert.IsTrue(mue3.GetMetadataType() == Event.MetadataUpdateEvent.MetadataType
					.Perms);
				NUnit.Framework.Assert.IsTrue(mue3.GetPerms().ToString().Contains("rw-rw-rw-"));
				// SetOwnerOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Metadata);
				Event.MetadataUpdateEvent mue4 = (Event.MetadataUpdateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(mue4.GetPath().Equals("/dir"));
				NUnit.Framework.Assert.IsTrue(mue4.GetMetadataType() == Event.MetadataUpdateEvent.MetadataType
					.Owner);
				NUnit.Framework.Assert.IsTrue(mue4.GetOwnerName().Equals("username"));
				NUnit.Framework.Assert.IsTrue(mue4.GetGroupName().Equals("groupname"));
				// SymlinkOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Create);
				Event.CreateEvent ce5 = (Event.CreateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(ce5.GetiNodeType() == Event.CreateEvent.INodeType.Symlink
					);
				NUnit.Framework.Assert.IsTrue(ce5.GetPath().Equals("/dir2"));
				NUnit.Framework.Assert.IsTrue(ce5.GetCtime() > 0);
				NUnit.Framework.Assert.IsTrue(ce5.GetReplication() == 0);
				NUnit.Framework.Assert.IsTrue(ce5.GetSymlinkTarget().Equals("/dir"));
				// SetXAttrOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Metadata);
				Event.MetadataUpdateEvent mue5 = (Event.MetadataUpdateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(mue5.GetPath().Equals("/file5"));
				NUnit.Framework.Assert.IsTrue(mue5.GetMetadataType() == Event.MetadataUpdateEvent.MetadataType
					.Xattrs);
				NUnit.Framework.Assert.IsTrue(mue5.GetxAttrs().Count == 1);
				NUnit.Framework.Assert.IsTrue(mue5.GetxAttrs()[0].GetName().Contains("field"));
				NUnit.Framework.Assert.IsTrue(!mue5.IsxAttrsRemoved());
				// RemoveXAttrOp
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Metadata);
				Event.MetadataUpdateEvent mue6 = (Event.MetadataUpdateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(mue6.GetPath().Equals("/file5"));
				NUnit.Framework.Assert.IsTrue(mue6.GetMetadataType() == Event.MetadataUpdateEvent.MetadataType
					.Xattrs);
				NUnit.Framework.Assert.IsTrue(mue6.GetxAttrs().Count == 1);
				NUnit.Framework.Assert.IsTrue(mue6.GetxAttrs()[0].GetName().Contains("field"));
				NUnit.Framework.Assert.IsTrue(mue6.IsxAttrsRemoved());
				// SetAclOp (1)
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Metadata);
				Event.MetadataUpdateEvent mue7 = (Event.MetadataUpdateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(mue7.GetPath().Equals("/file5"));
				NUnit.Framework.Assert.IsTrue(mue7.GetMetadataType() == Event.MetadataUpdateEvent.MetadataType
					.Acls);
				NUnit.Framework.Assert.IsTrue(mue7.GetAcls().Contains(AclEntry.ParseAclEntry("user::rwx"
					, true)));
				// SetAclOp (2)
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Metadata);
				Event.MetadataUpdateEvent mue8 = (Event.MetadataUpdateEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(mue8.GetPath().Equals("/file5"));
				NUnit.Framework.Assert.IsTrue(mue8.GetMetadataType() == Event.MetadataUpdateEvent.MetadataType
					.Acls);
				NUnit.Framework.Assert.IsTrue(mue8.GetAcls() == null);
				// RenameOp (2)
				batch = WaitForNextEvents(eis);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				txid = CheckTxid(batch, txid);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Rename);
				Event.RenameEvent re3 = (Event.RenameEvent)batch.GetEvents()[0];
				NUnit.Framework.Assert.IsTrue(re3.GetDstPath().Equals("/dir/file5"));
				NUnit.Framework.Assert.IsTrue(re3.GetSrcPath().Equals("/file5"));
				NUnit.Framework.Assert.IsTrue(re.GetTimestamp() > 0);
				// Returns null when there are no further events
				NUnit.Framework.Assert.IsTrue(eis.Poll() == null);
				// make sure the estimate hasn't changed since the above assertion
				// tells us that we are fully caught up to the current namesystem state
				// and we should not have been behind at all when eventsBehind was set
				// either, since there were few enough events that they should have all
				// been read to the client during the first poll() call
				NUnit.Framework.Assert.IsTrue(eis.GetTxidsBehindEstimate() == eventsBehind);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException"/>
		public virtual void TestNNFailover()
		{
			Configuration conf = new HdfsConfiguration();
			MiniQJMHACluster cluster = new MiniQJMHACluster.Builder(conf).Build();
			try
			{
				cluster.GetDfsCluster().WaitActive();
				cluster.GetDfsCluster().TransitionToActive(0);
				DFSClient client = ((DistributedFileSystem)HATestUtil.ConfigureFailoverFs(cluster
					.GetDfsCluster(), conf)).dfs;
				DFSInotifyEventInputStream eis = client.GetInotifyEventStream();
				for (int i = 0; i < 10; i++)
				{
					client.Mkdirs("/dir" + i, null, false);
				}
				cluster.GetDfsCluster().ShutdownNameNode(0);
				cluster.GetDfsCluster().TransitionToActive(1);
				EventBatch batch = null;
				// we can read all of the edits logged by the old active from the new
				// active
				for (int i_1 = 0; i_1 < 10; i_1++)
				{
					batch = WaitForNextEvents(eis);
					NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
					NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
						.Create);
					NUnit.Framework.Assert.IsTrue(((Event.CreateEvent)batch.GetEvents()[0]).GetPath()
						.Equals("/dir" + i_1));
				}
				NUnit.Framework.Assert.IsTrue(eis.Poll() == null);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException"/>
		public virtual void TestTwoActiveNNs()
		{
			Configuration conf = new HdfsConfiguration();
			MiniQJMHACluster cluster = new MiniQJMHACluster.Builder(conf).Build();
			try
			{
				cluster.GetDfsCluster().WaitActive();
				cluster.GetDfsCluster().TransitionToActive(0);
				DFSClient client0 = new DFSClient(cluster.GetDfsCluster().GetNameNode(0).GetNameNodeAddress
					(), conf);
				DFSClient client1 = new DFSClient(cluster.GetDfsCluster().GetNameNode(1).GetNameNodeAddress
					(), conf);
				DFSInotifyEventInputStream eis = client0.GetInotifyEventStream();
				for (int i = 0; i < 10; i++)
				{
					client0.Mkdirs("/dir" + i, null, false);
				}
				cluster.GetDfsCluster().TransitionToActive(1);
				for (int i_1 = 10; i_1 < 20; i_1++)
				{
					client1.Mkdirs("/dir" + i_1, null, false);
				}
				// make sure that the old active can't read any further than the edits
				// it logged itself (it has no idea whether the in-progress edits from
				// the other writer have actually been committed)
				EventBatch batch = null;
				for (int i_2 = 0; i_2 < 10; i_2++)
				{
					batch = WaitForNextEvents(eis);
					NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
					NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
						.Create);
					NUnit.Framework.Assert.IsTrue(((Event.CreateEvent)batch.GetEvents()[0]).GetPath()
						.Equals("/dir" + i_2));
				}
				NUnit.Framework.Assert.IsTrue(eis.Poll() == null);
			}
			finally
			{
				try
				{
					cluster.Shutdown();
				}
				catch (ExitUtil.ExitException)
				{
				}
			}
		}

		// expected because the old active will be unable to flush the
		// end-of-segment op since it is fenced
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Inotify.MissingEventsException"/>
		public virtual void TestReadEventsWithTimeout()
		{
			Configuration conf = new HdfsConfiguration();
			MiniQJMHACluster cluster = new MiniQJMHACluster.Builder(conf).Build();
			try
			{
				cluster.GetDfsCluster().WaitActive();
				cluster.GetDfsCluster().TransitionToActive(0);
				DFSClient client = new DFSClient(cluster.GetDfsCluster().GetNameNode(0).GetNameNodeAddress
					(), conf);
				DFSInotifyEventInputStream eis = client.GetInotifyEventStream();
				ScheduledExecutorService ex = Executors.NewSingleThreadScheduledExecutor();
				ex.Schedule(new _Runnable_463(client), 1, TimeUnit.Seconds);
				// test will fail
				// a very generous wait period -- the edit will definitely have been
				// processed by the time this is up
				EventBatch batch = eis.Poll(5, TimeUnit.Seconds);
				NUnit.Framework.Assert.IsNotNull(batch);
				NUnit.Framework.Assert.AreEqual(1, batch.GetEvents().Length);
				NUnit.Framework.Assert.IsTrue(batch.GetEvents()[0].GetEventType() == Event.EventType
					.Create);
				NUnit.Framework.Assert.AreEqual("/dir", ((Event.CreateEvent)batch.GetEvents()[0])
					.GetPath());
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _Runnable_463 : Runnable
		{
			public _Runnable_463(DFSClient client)
			{
				this.client = client;
			}

			public void Run()
			{
				try
				{
					client.Mkdirs("/dir", null, false);
				}
				catch (IOException e)
				{
					TestDFSInotifyEventInputStream.Log.Error("Unable to create /dir", e);
				}
			}

			private readonly DFSClient client;
		}
	}
}
