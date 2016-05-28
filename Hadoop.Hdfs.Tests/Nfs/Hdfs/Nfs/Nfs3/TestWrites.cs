using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3.Request;
using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	public class TestWrites
	{
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAlterWriteRequest()
		{
			int len = 20;
			byte[] data = new byte[len];
			ByteBuffer buffer = ByteBuffer.Wrap(data);
			for (int i = 0; i < len; i++)
			{
				buffer.Put(unchecked((byte)i));
			}
			buffer.Flip();
			int originalCount = ((byte[])buffer.Array()).Length;
			WRITE3Request request = new WRITE3Request(new FileHandle(), 0, data.Length, Nfs3Constant.WriteStableHow
				.Unstable, buffer);
			WriteCtx writeCtx1 = new WriteCtx(request.GetHandle(), request.GetOffset(), request
				.GetCount(), WriteCtx.InvalidOriginalCount, request.GetStableHow(), request.GetData
				(), null, 1, false, WriteCtx.DataState.NoDump);
			NUnit.Framework.Assert.IsTrue(((byte[])writeCtx1.GetData().Array()).Length == originalCount
				);
			// Now change the write request
			OpenFileCtx.AlterWriteRequest(request, 12);
			WriteCtx writeCtx2 = new WriteCtx(request.GetHandle(), request.GetOffset(), request
				.GetCount(), originalCount, request.GetStableHow(), request.GetData(), null, 2, 
				false, WriteCtx.DataState.NoDump);
			ByteBuffer appendedData = writeCtx2.GetData();
			int position = appendedData.Position();
			int limit = appendedData.Limit();
			NUnit.Framework.Assert.IsTrue(position == 12);
			NUnit.Framework.Assert.IsTrue(limit - position == 8);
			NUnit.Framework.Assert.IsTrue(appendedData.Get(position) == unchecked((byte)12));
			NUnit.Framework.Assert.IsTrue(appendedData.Get(position + 1) == unchecked((byte)13
				));
			NUnit.Framework.Assert.IsTrue(appendedData.Get(position + 2) == unchecked((byte)14
				));
			NUnit.Framework.Assert.IsTrue(appendedData.Get(position + 7) == unchecked((byte)19
				));
			// Test current file write offset is at boundaries
			buffer.Position(0);
			request = new WRITE3Request(new FileHandle(), 0, data.Length, Nfs3Constant.WriteStableHow
				.Unstable, buffer);
			OpenFileCtx.AlterWriteRequest(request, 1);
			WriteCtx writeCtx3 = new WriteCtx(request.GetHandle(), request.GetOffset(), request
				.GetCount(), originalCount, request.GetStableHow(), request.GetData(), null, 2, 
				false, WriteCtx.DataState.NoDump);
			appendedData = writeCtx3.GetData();
			position = appendedData.Position();
			limit = appendedData.Limit();
			NUnit.Framework.Assert.IsTrue(position == 1);
			NUnit.Framework.Assert.IsTrue(limit - position == 19);
			NUnit.Framework.Assert.IsTrue(appendedData.Get(position) == unchecked((byte)1));
			NUnit.Framework.Assert.IsTrue(appendedData.Get(position + 18) == unchecked((byte)
				19));
			// Reset buffer position before test another boundary
			buffer.Position(0);
			request = new WRITE3Request(new FileHandle(), 0, data.Length, Nfs3Constant.WriteStableHow
				.Unstable, buffer);
			OpenFileCtx.AlterWriteRequest(request, 19);
			WriteCtx writeCtx4 = new WriteCtx(request.GetHandle(), request.GetOffset(), request
				.GetCount(), originalCount, request.GetStableHow(), request.GetData(), null, 2, 
				false, WriteCtx.DataState.NoDump);
			appendedData = writeCtx4.GetData();
			position = appendedData.Position();
			limit = appendedData.Limit();
			NUnit.Framework.Assert.IsTrue(position == 19);
			NUnit.Framework.Assert.IsTrue(limit - position == 1);
			NUnit.Framework.Assert.IsTrue(appendedData.Get(position) == unchecked((byte)19));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckCommit()
		{
			// Validate all the commit check return codes OpenFileCtx.COMMIT_STATUS, which
			// includes COMMIT_FINISHED, COMMIT_WAIT, COMMIT_INACTIVE_CTX,
			// COMMIT_INACTIVE_WITH_PENDING_WRITE, COMMIT_ERROR, and COMMIT_DO_SYNC.
			DFSClient dfsClient = Org.Mockito.Mockito.Mock<DFSClient>();
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			HdfsDataOutputStream fos = Org.Mockito.Mockito.Mock<HdfsDataOutputStream>();
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)0);
			NfsConfiguration conf = new NfsConfiguration();
			conf.SetBoolean(NfsConfigKeys.LargeFileUpload, false);
			OpenFileCtx ctx = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new ShellBasedIdMapping
				(conf), false, conf);
			OpenFileCtx.COMMIT_STATUS ret;
			// Test inactive open file context
			ctx.SetActiveStatusForTest(false);
			Org.Jboss.Netty.Channel.Channel ch = Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel
				>();
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitInactiveCtx);
			ctx.GetPendingWritesForTest()[new OffsetRange(5, 10)] = new WriteCtx(null, 0, 0, 
				0, null, null, null, 0, false, null);
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitInactiveWithPendingWrite
				);
			// Test request with non zero commit offset
			ctx.SetActiveStatusForTest(true);
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)10);
			ctx.SetNextOffsetForTest(10);
			OpenFileCtx.COMMIT_STATUS status = ctx.CheckCommitInternal(5, null, 1, attr, false
				);
			NUnit.Framework.Assert.IsTrue(status == OpenFileCtx.COMMIT_STATUS.CommitDoSync);
			// Do_SYNC state will be updated to FINISHED after data sync
			ret = ctx.CheckCommit(dfsClient, 5, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitFinished);
			status = ctx.CheckCommitInternal(10, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(status == OpenFileCtx.COMMIT_STATUS.CommitDoSync);
			ret = ctx.CheckCommit(dfsClient, 10, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitFinished);
			ConcurrentNavigableMap<long, OpenFileCtx.CommitCtx> commits = ctx.GetPendingCommitsForTest
				();
			NUnit.Framework.Assert.IsTrue(commits.Count == 0);
			ret = ctx.CheckCommit(dfsClient, 11, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitWait);
			NUnit.Framework.Assert.IsTrue(commits.Count == 1);
			long key = commits.FirstKey();
			NUnit.Framework.Assert.IsTrue(key == 11);
			// Test request with zero commit offset
			Sharpen.Collections.Remove(commits, System.Convert.ToInt64(11));
			// There is one pending write [5,10]
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitWait);
			NUnit.Framework.Assert.IsTrue(commits.Count == 1);
			key = commits.FirstKey();
			NUnit.Framework.Assert.IsTrue(key == 9);
			// Empty pending writes
			Sharpen.Collections.Remove(ctx.GetPendingWritesForTest(), new OffsetRange(5, 10));
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitFinished);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckCommitLargeFileUpload()
		{
			// Validate all the commit check return codes OpenFileCtx.COMMIT_STATUS with
			// large file upload option.
			DFSClient dfsClient = Org.Mockito.Mockito.Mock<DFSClient>();
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			HdfsDataOutputStream fos = Org.Mockito.Mockito.Mock<HdfsDataOutputStream>();
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)0);
			NfsConfiguration conf = new NfsConfiguration();
			conf.SetBoolean(NfsConfigKeys.LargeFileUpload, true);
			OpenFileCtx ctx = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new ShellBasedIdMapping
				(conf), false, conf);
			OpenFileCtx.COMMIT_STATUS ret;
			// Test inactive open file context
			ctx.SetActiveStatusForTest(false);
			Org.Jboss.Netty.Channel.Channel ch = Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel
				>();
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitInactiveCtx);
			ctx.GetPendingWritesForTest()[new OffsetRange(10, 15)] = new WriteCtx(null, 0, 0, 
				0, null, null, null, 0, false, null);
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitInactiveWithPendingWrite
				);
			// Test request with non zero commit offset
			ctx.SetActiveStatusForTest(true);
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)8);
			ctx.SetNextOffsetForTest(10);
			OpenFileCtx.COMMIT_STATUS status = ctx.CheckCommitInternal(5, null, 1, attr, false
				);
			NUnit.Framework.Assert.IsTrue(status == OpenFileCtx.COMMIT_STATUS.CommitDoSync);
			// Do_SYNC state will be updated to FINISHED after data sync
			ret = ctx.CheckCommit(dfsClient, 5, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitFinished);
			// Test commit sequential writes
			status = ctx.CheckCommitInternal(10, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(status == OpenFileCtx.COMMIT_STATUS.CommitSpecialWait
				);
			ret = ctx.CheckCommit(dfsClient, 10, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitSpecialWait);
			// Test commit non-sequential writes
			ConcurrentNavigableMap<long, OpenFileCtx.CommitCtx> commits = ctx.GetPendingCommitsForTest
				();
			NUnit.Framework.Assert.IsTrue(commits.Count == 1);
			ret = ctx.CheckCommit(dfsClient, 16, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitSpecialSuccess
				);
			NUnit.Framework.Assert.IsTrue(commits.Count == 1);
			// Test request with zero commit offset
			Sharpen.Collections.Remove(commits, System.Convert.ToInt64(10));
			// There is one pending write [10,15]
			ret = ctx.CheckCommitInternal(0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitSpecialWait);
			ret = ctx.CheckCommitInternal(9, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitSpecialWait);
			NUnit.Framework.Assert.IsTrue(commits.Count == 2);
			// Empty pending writes. nextOffset=10, flushed pos=8
			Sharpen.Collections.Remove(ctx.GetPendingWritesForTest(), new OffsetRange(10, 15)
				);
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitSpecialWait);
			// Empty pending writes
			ctx.SetNextOffsetForTest((long)8);
			// flushed pos = 8
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(ret == OpenFileCtx.COMMIT_STATUS.CommitFinished);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckCommitAixCompatMode()
		{
			DFSClient dfsClient = Org.Mockito.Mockito.Mock<DFSClient>();
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			HdfsDataOutputStream fos = Org.Mockito.Mockito.Mock<HdfsDataOutputStream>();
			NfsConfiguration conf = new NfsConfiguration();
			conf.SetBoolean(NfsConfigKeys.LargeFileUpload, false);
			// Enable AIX compatibility mode.
			OpenFileCtx ctx = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new ShellBasedIdMapping
				(new NfsConfiguration()), true, conf);
			// Test fall-through to pendingWrites check in the event that commitOffset
			// is greater than the number of bytes we've so far flushed.
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)2);
			OpenFileCtx.COMMIT_STATUS status = ctx.CheckCommitInternal(5, null, 1, attr, false
				);
			NUnit.Framework.Assert.IsTrue(status == OpenFileCtx.COMMIT_STATUS.CommitFinished);
			// Test the case when we actually have received more bytes than we're trying
			// to commit.
			ctx.GetPendingWritesForTest()[new OffsetRange(0, 10)] = new WriteCtx(null, 0, 0, 
				0, null, null, null, 0, false, null);
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)10);
			ctx.SetNextOffsetForTest((long)10);
			status = ctx.CheckCommitInternal(5, null, 1, attr, false);
			NUnit.Framework.Assert.IsTrue(status == OpenFileCtx.COMMIT_STATUS.CommitDoSync);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckCommitFromRead()
		{
			// Validate all the commit check return codes OpenFileCtx.COMMIT_STATUS, which
			// includes COMMIT_FINISHED, COMMIT_WAIT, COMMIT_INACTIVE_CTX,
			// COMMIT_INACTIVE_WITH_PENDING_WRITE, COMMIT_ERROR, and COMMIT_DO_SYNC.
			DFSClient dfsClient = Org.Mockito.Mockito.Mock<DFSClient>();
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			HdfsDataOutputStream fos = Org.Mockito.Mockito.Mock<HdfsDataOutputStream>();
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)0);
			NfsConfiguration config = new NfsConfiguration();
			config.SetBoolean(NfsConfigKeys.LargeFileUpload, false);
			OpenFileCtx ctx = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new ShellBasedIdMapping
				(config), false, config);
			FileHandle h = new FileHandle(1);
			// fake handle for "/dumpFilePath"
			OpenFileCtx.COMMIT_STATUS ret;
			WriteManager wm = new WriteManager(new ShellBasedIdMapping(config), config, false
				);
			NUnit.Framework.Assert.IsTrue(wm.AddOpenFileStream(h, ctx));
			// Test inactive open file context
			ctx.SetActiveStatusForTest(false);
			Org.Jboss.Netty.Channel.Channel ch = Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel
				>();
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitInactiveCtx, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3Ok, wm.CommitBeforeRead(dfsClient, 
				h, 0));
			ctx.GetPendingWritesForTest()[new OffsetRange(10, 15)] = new WriteCtx(null, 0, 0, 
				0, null, null, null, 0, false, null);
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitInactiveWithPendingWrite
				, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3errIo, wm.CommitBeforeRead(dfsClient
				, h, 0));
			// Test request with non zero commit offset
			ctx.SetActiveStatusForTest(true);
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)10);
			ctx.SetNextOffsetForTest((long)10);
			OpenFileCtx.COMMIT_STATUS status = ctx.CheckCommitInternal(5, ch, 1, attr, false);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitDoSync, status);
			// Do_SYNC state will be updated to FINISHED after data sync
			ret = ctx.CheckCommit(dfsClient, 5, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitFinished, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3Ok, wm.CommitBeforeRead(dfsClient, 
				h, 5));
			status = ctx.CheckCommitInternal(10, ch, 1, attr, true);
			NUnit.Framework.Assert.IsTrue(status == OpenFileCtx.COMMIT_STATUS.CommitDoSync);
			ret = ctx.CheckCommit(dfsClient, 10, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitFinished, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3Ok, wm.CommitBeforeRead(dfsClient, 
				h, 10));
			ConcurrentNavigableMap<long, OpenFileCtx.CommitCtx> commits = ctx.GetPendingCommitsForTest
				();
			NUnit.Framework.Assert.IsTrue(commits.Count == 0);
			ret = ctx.CheckCommit(dfsClient, 11, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitWait, ret);
			NUnit.Framework.Assert.AreEqual(0, commits.Count);
			// commit triggered by read doesn't wait
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3errJukebox, wm.CommitBeforeRead(dfsClient
				, h, 11));
			// Test request with zero commit offset
			// There is one pending write [5,10]
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitWait, ret);
			NUnit.Framework.Assert.AreEqual(0, commits.Count);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3errJukebox, wm.CommitBeforeRead(dfsClient
				, h, 0));
			// Empty pending writes
			Sharpen.Collections.Remove(ctx.GetPendingWritesForTest(), new OffsetRange(10, 15)
				);
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitFinished, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3Ok, wm.CommitBeforeRead(dfsClient, 
				h, 0));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckCommitFromReadLargeFileUpload()
		{
			// Validate all the commit check return codes OpenFileCtx.COMMIT_STATUS with large file upload option
			DFSClient dfsClient = Org.Mockito.Mockito.Mock<DFSClient>();
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			HdfsDataOutputStream fos = Org.Mockito.Mockito.Mock<HdfsDataOutputStream>();
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)0);
			NfsConfiguration config = new NfsConfiguration();
			config.SetBoolean(NfsConfigKeys.LargeFileUpload, true);
			OpenFileCtx ctx = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new ShellBasedIdMapping
				(config), false, config);
			FileHandle h = new FileHandle(1);
			// fake handle for "/dumpFilePath"
			OpenFileCtx.COMMIT_STATUS ret;
			WriteManager wm = new WriteManager(new ShellBasedIdMapping(config), config, false
				);
			NUnit.Framework.Assert.IsTrue(wm.AddOpenFileStream(h, ctx));
			// Test inactive open file context
			ctx.SetActiveStatusForTest(false);
			Org.Jboss.Netty.Channel.Channel ch = Org.Mockito.Mockito.Mock<Org.Jboss.Netty.Channel.Channel
				>();
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitInactiveCtx, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3Ok, wm.CommitBeforeRead(dfsClient, 
				h, 0));
			ctx.GetPendingWritesForTest()[new OffsetRange(10, 15)] = new WriteCtx(null, 0, 0, 
				0, null, null, null, 0, false, null);
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitInactiveWithPendingWrite
				, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3errIo, wm.CommitBeforeRead(dfsClient
				, h, 0));
			// Test request with non zero commit offset
			ctx.SetActiveStatusForTest(true);
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)6);
			ctx.SetNextOffsetForTest((long)10);
			OpenFileCtx.COMMIT_STATUS status = ctx.CheckCommitInternal(5, ch, 1, attr, false);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitDoSync, status);
			// Do_SYNC state will be updated to FINISHED after data sync
			ret = ctx.CheckCommit(dfsClient, 5, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitFinished, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3Ok, wm.CommitBeforeRead(dfsClient, 
				h, 5));
			// Test request with sequential writes
			status = ctx.CheckCommitInternal(9, ch, 1, attr, true);
			NUnit.Framework.Assert.IsTrue(status == OpenFileCtx.COMMIT_STATUS.CommitSpecialWait
				);
			ret = ctx.CheckCommit(dfsClient, 9, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitSpecialWait, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3errJukebox, wm.CommitBeforeRead(dfsClient
				, h, 9));
			// Test request with non-sequential writes
			ConcurrentNavigableMap<long, OpenFileCtx.CommitCtx> commits = ctx.GetPendingCommitsForTest
				();
			NUnit.Framework.Assert.IsTrue(commits.Count == 0);
			ret = ctx.CheckCommit(dfsClient, 16, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitSpecialSuccess, ret
				);
			NUnit.Framework.Assert.AreEqual(0, commits.Count);
			// commit triggered by read doesn't wait
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3Ok, wm.CommitBeforeRead(dfsClient, 
				h, 16));
			// Test request with zero commit offset
			// There is one pending write [10,15]
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitSpecialWait, ret);
			NUnit.Framework.Assert.AreEqual(0, commits.Count);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3errJukebox, wm.CommitBeforeRead(dfsClient
				, h, 0));
			// Empty pending writes
			Sharpen.Collections.Remove(ctx.GetPendingWritesForTest(), new OffsetRange(10, 15)
				);
			ret = ctx.CheckCommit(dfsClient, 0, ch, 1, attr, true);
			NUnit.Framework.Assert.AreEqual(OpenFileCtx.COMMIT_STATUS.CommitSpecialWait, ret);
			NUnit.Framework.Assert.AreEqual(Nfs3Status.Nfs3errJukebox, wm.CommitBeforeRead(dfsClient
				, h, 0));
		}

		/// <exception cref="System.Exception"/>
		private void WaitWrite(RpcProgramNfs3 nfsd, FileHandle handle, int maxWaitTime)
		{
			int waitedTime = 0;
			OpenFileCtx ctx = nfsd.GetWriteManager().GetOpenFileCtxCache().Get(handle);
			NUnit.Framework.Assert.IsTrue(ctx != null);
			do
			{
				Sharpen.Thread.Sleep(3000);
				waitedTime += 3000;
				if (ctx.GetPendingWritesForTest().Count == 0)
				{
					return;
				}
			}
			while (waitedTime < maxWaitTime);
			NUnit.Framework.Assert.Fail("Write can't finish.");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteStableHow()
		{
			NfsConfiguration config = new NfsConfiguration();
			DFSClient client = null;
			MiniDFSCluster cluster = null;
			RpcProgramNfs3 nfsd;
			SecurityHandler securityHandler = Org.Mockito.Mockito.Mock<SecurityHandler>();
			Org.Mockito.Mockito.When(securityHandler.GetUser()).ThenReturn(Runtime.GetProperty
				("user.name"));
			string currentUser = Runtime.GetProperty("user.name");
			config.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(currentUser), "*");
			config.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(currentUser), "*");
			ProxyUsers.RefreshSuperUserGroupsConfiguration(config);
			try
			{
				cluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).Build();
				cluster.WaitActive();
				client = new DFSClient(NameNode.GetAddress(config), config);
				// Use emphral port in case tests are running in parallel
				config.SetInt("nfs3.mountd.port", 0);
				config.SetInt("nfs3.server.port", 0);
				// Start nfs
				Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfs3 = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3
					(config);
				nfs3.StartServiceInternal(false);
				nfsd = (RpcProgramNfs3)nfs3.GetRpcProgram();
				HdfsFileStatus status = client.GetFileInfo("/");
				FileHandle rootHandle = new FileHandle(status.GetFileId());
				// Create file1
				CREATE3Request createReq = new CREATE3Request(rootHandle, "file1", Nfs3Constant.CreateUnchecked
					, new SetAttr3(), 0);
				XDR createXdr = new XDR();
				createReq.Serialize(createXdr);
				CREATE3Response createRsp = nfsd.Create(createXdr.AsReadOnlyWrap(), securityHandler
					, new IPEndPoint("localhost", 1234));
				FileHandle handle = createRsp.GetObjHandle();
				// Test DATA_SYNC
				byte[] buffer = new byte[10];
				for (int i = 0; i < 10; i++)
				{
					buffer[i] = unchecked((byte)i);
				}
				WRITE3Request writeReq = new WRITE3Request(handle, 0, 10, Nfs3Constant.WriteStableHow
					.DataSync, ByteBuffer.Wrap(buffer));
				XDR writeXdr = new XDR();
				writeReq.Serialize(writeXdr);
				nfsd.Write(writeXdr.AsReadOnlyWrap(), null, 1, securityHandler, new IPEndPoint("localhost"
					, 1234));
				WaitWrite(nfsd, handle, 60000);
				// Readback
				READ3Request readReq = new READ3Request(handle, 0, 10);
				XDR readXdr = new XDR();
				readReq.Serialize(readXdr);
				READ3Response readRsp = nfsd.Read(readXdr.AsReadOnlyWrap(), securityHandler, new 
					IPEndPoint("localhost", 1234));
				NUnit.Framework.Assert.IsTrue(Arrays.Equals(buffer, ((byte[])readRsp.GetData().Array
					())));
				// Test FILE_SYNC
				// Create file2
				CREATE3Request createReq2 = new CREATE3Request(rootHandle, "file2", Nfs3Constant.
					CreateUnchecked, new SetAttr3(), 0);
				XDR createXdr2 = new XDR();
				createReq2.Serialize(createXdr2);
				CREATE3Response createRsp2 = nfsd.Create(createXdr2.AsReadOnlyWrap(), securityHandler
					, new IPEndPoint("localhost", 1234));
				FileHandle handle2 = createRsp2.GetObjHandle();
				WRITE3Request writeReq2 = new WRITE3Request(handle2, 0, 10, Nfs3Constant.WriteStableHow
					.FileSync, ByteBuffer.Wrap(buffer));
				XDR writeXdr2 = new XDR();
				writeReq2.Serialize(writeXdr2);
				nfsd.Write(writeXdr2.AsReadOnlyWrap(), null, 1, securityHandler, new IPEndPoint("localhost"
					, 1234));
				WaitWrite(nfsd, handle2, 60000);
				// Readback
				READ3Request readReq2 = new READ3Request(handle2, 0, 10);
				XDR readXdr2 = new XDR();
				readReq2.Serialize(readXdr2);
				READ3Response readRsp2 = nfsd.Read(readXdr2.AsReadOnlyWrap(), securityHandler, new 
					IPEndPoint("localhost", 1234));
				NUnit.Framework.Assert.IsTrue(Arrays.Equals(buffer, ((byte[])readRsp2.GetData().Array
					())));
				// FILE_SYNC should sync the file size
				status = client.GetFileInfo("/file2");
				NUnit.Framework.Assert.IsTrue(status.GetLen() == 10);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOOOWrites()
		{
			NfsConfiguration config = new NfsConfiguration();
			MiniDFSCluster cluster = null;
			RpcProgramNfs3 nfsd;
			int bufSize = 32;
			int numOOO = 3;
			SecurityHandler securityHandler = Org.Mockito.Mockito.Mock<SecurityHandler>();
			Org.Mockito.Mockito.When(securityHandler.GetUser()).ThenReturn(Runtime.GetProperty
				("user.name"));
			string currentUser = Runtime.GetProperty("user.name");
			config.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserGroupConfKey
				(currentUser), "*");
			config.Set(DefaultImpersonationProvider.GetTestProvider().GetProxySuperuserIpConfKey
				(currentUser), "*");
			ProxyUsers.RefreshSuperUserGroupsConfiguration(config);
			// Use emphral port in case tests are running in parallel
			config.SetInt("nfs3.mountd.port", 0);
			config.SetInt("nfs3.server.port", 0);
			try
			{
				cluster = new MiniDFSCluster.Builder(config).NumDataNodes(1).Build();
				cluster.WaitActive();
				Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3 nfs3 = new Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.Nfs3
					(config);
				nfs3.StartServiceInternal(false);
				nfsd = (RpcProgramNfs3)nfs3.GetRpcProgram();
				DFSClient dfsClient = new DFSClient(NameNode.GetAddress(config), config);
				HdfsFileStatus status = dfsClient.GetFileInfo("/");
				FileHandle rootHandle = new FileHandle(status.GetFileId());
				CREATE3Request createReq = new CREATE3Request(rootHandle, "out-of-order-write" + 
					Runtime.CurrentTimeMillis(), Nfs3Constant.CreateUnchecked, new SetAttr3(), 0);
				XDR createXdr = new XDR();
				createReq.Serialize(createXdr);
				CREATE3Response createRsp = nfsd.Create(createXdr.AsReadOnlyWrap(), securityHandler
					, new IPEndPoint("localhost", 1234));
				FileHandle handle = createRsp.GetObjHandle();
				byte[][] oooBuf = new byte[][] { new byte[bufSize], new byte[bufSize], new byte[bufSize
					] };
				for (int i = 0; i < numOOO; i++)
				{
					Arrays.Fill(oooBuf[i], unchecked((byte)i));
				}
				for (int i_1 = 0; i_1 < numOOO; i_1++)
				{
					long offset = (numOOO - 1 - i_1) * bufSize;
					WRITE3Request writeReq = new WRITE3Request(handle, offset, bufSize, Nfs3Constant.WriteStableHow
						.Unstable, ByteBuffer.Wrap(oooBuf[i_1]));
					XDR writeXdr = new XDR();
					writeReq.Serialize(writeXdr);
					nfsd.Write(writeXdr.AsReadOnlyWrap(), null, 1, securityHandler, new IPEndPoint("localhost"
						, 1234));
				}
				WaitWrite(nfsd, handle, 60000);
				READ3Request readReq = new READ3Request(handle, bufSize, bufSize);
				XDR readXdr = new XDR();
				readReq.Serialize(readXdr);
				READ3Response readRsp = nfsd.Read(readXdr.AsReadOnlyWrap(), securityHandler, new 
					IPEndPoint("localhost", config.GetInt(NfsConfigKeys.DfsNfsServerPortKey, NfsConfigKeys
					.DfsNfsServerPortDefault)));
				NUnit.Framework.Assert.IsTrue(Arrays.Equals(oooBuf[1], ((byte[])readRsp.GetData()
					.Array())));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckSequential()
		{
			DFSClient dfsClient = Org.Mockito.Mockito.Mock<DFSClient>();
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			HdfsDataOutputStream fos = Org.Mockito.Mockito.Mock<HdfsDataOutputStream>();
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)0);
			NfsConfiguration config = new NfsConfiguration();
			config.SetBoolean(NfsConfigKeys.LargeFileUpload, false);
			OpenFileCtx ctx = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new ShellBasedIdMapping
				(config), false, config);
			ctx.GetPendingWritesForTest()[new OffsetRange(5, 10)] = new WriteCtx(null, 0, 0, 
				0, null, null, null, 0, false, null);
			ctx.GetPendingWritesForTest()[new OffsetRange(10, 15)] = new WriteCtx(null, 0, 0, 
				0, null, null, null, 0, false, null);
			ctx.GetPendingWritesForTest()[new OffsetRange(20, 25)] = new WriteCtx(null, 0, 0, 
				0, null, null, null, 0, false, null);
			NUnit.Framework.Assert.IsTrue(!ctx.CheckSequential(5, 4));
			NUnit.Framework.Assert.IsTrue(ctx.CheckSequential(9, 5));
			NUnit.Framework.Assert.IsTrue(ctx.CheckSequential(10, 5));
			NUnit.Framework.Assert.IsTrue(ctx.CheckSequential(14, 5));
			NUnit.Framework.Assert.IsTrue(!ctx.CheckSequential(15, 5));
			NUnit.Framework.Assert.IsTrue(!ctx.CheckSequential(20, 5));
			NUnit.Framework.Assert.IsTrue(!ctx.CheckSequential(25, 5));
			NUnit.Framework.Assert.IsTrue(!ctx.CheckSequential(999, 5));
		}
	}
}
