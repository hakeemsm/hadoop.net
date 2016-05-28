using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	public class TestOpenFileCtxCache
	{
		internal static bool cleaned = false;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEviction()
		{
			NfsConfiguration conf = new NfsConfiguration();
			// Only two entries will be in the cache
			conf.SetInt(NfsConfigKeys.DfsNfsMaxOpenFilesKey, 2);
			DFSClient dfsClient = Org.Mockito.Mockito.Mock<DFSClient>();
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			HdfsDataOutputStream fos = Org.Mockito.Mockito.Mock<HdfsDataOutputStream>();
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)0);
			OpenFileCtx context1 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtx context2 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtx context3 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtx context4 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtx context5 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtxCache cache = new OpenFileCtxCache(conf, 10 * 60 * 100);
			bool ret = cache.Put(new FileHandle(1), context1);
			NUnit.Framework.Assert.IsTrue(ret);
			Sharpen.Thread.Sleep(1000);
			ret = cache.Put(new FileHandle(2), context2);
			NUnit.Framework.Assert.IsTrue(ret);
			ret = cache.Put(new FileHandle(3), context3);
			NUnit.Framework.Assert.IsFalse(ret);
			NUnit.Framework.Assert.IsTrue(cache.Size() == 2);
			// Wait for the oldest stream to be evict-able, insert again
			Sharpen.Thread.Sleep(NfsConfigKeys.DfsNfsStreamTimeoutMinDefault);
			NUnit.Framework.Assert.IsTrue(cache.Size() == 2);
			ret = cache.Put(new FileHandle(3), context3);
			NUnit.Framework.Assert.IsTrue(ret);
			NUnit.Framework.Assert.IsTrue(cache.Size() == 2);
			NUnit.Framework.Assert.IsTrue(cache.Get(new FileHandle(1)) == null);
			// Test inactive entry is evicted immediately
			context3.SetActiveStatusForTest(false);
			ret = cache.Put(new FileHandle(4), context4);
			NUnit.Framework.Assert.IsTrue(ret);
			// Now the cache has context2 and context4
			// Test eviction failure if all entries have pending work.
			context2.GetPendingWritesForTest()[new OffsetRange(0, 100)] = new WriteCtx(null, 
				0, 0, 0, null, null, null, 0, false, null);
			context4.GetPendingCommitsForTest()[System.Convert.ToInt64(100)] = new OpenFileCtx.CommitCtx
				(0, null, 0, attr);
			Sharpen.Thread.Sleep(NfsConfigKeys.DfsNfsStreamTimeoutMinDefault);
			ret = cache.Put(new FileHandle(5), context5);
			NUnit.Framework.Assert.IsFalse(ret);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestScan()
		{
			NfsConfiguration conf = new NfsConfiguration();
			// Only two entries will be in the cache
			conf.SetInt(NfsConfigKeys.DfsNfsMaxOpenFilesKey, 2);
			DFSClient dfsClient = Org.Mockito.Mockito.Mock<DFSClient>();
			Nfs3FileAttributes attr = new Nfs3FileAttributes();
			HdfsDataOutputStream fos = Org.Mockito.Mockito.Mock<HdfsDataOutputStream>();
			Org.Mockito.Mockito.When(fos.GetPos()).ThenReturn((long)0);
			OpenFileCtx context1 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtx context2 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtx context3 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtx context4 = new OpenFileCtx(fos, attr, "/dumpFilePath", dfsClient, new 
				ShellBasedIdMapping(new NfsConfiguration()));
			OpenFileCtxCache cache = new OpenFileCtxCache(conf, 10 * 60 * 100);
			// Test cleaning expired entry
			bool ret = cache.Put(new FileHandle(1), context1);
			NUnit.Framework.Assert.IsTrue(ret);
			ret = cache.Put(new FileHandle(2), context2);
			NUnit.Framework.Assert.IsTrue(ret);
			Sharpen.Thread.Sleep(NfsConfigKeys.DfsNfsStreamTimeoutMinDefault + 1);
			cache.Scan(NfsConfigKeys.DfsNfsStreamTimeoutMinDefault);
			NUnit.Framework.Assert.IsTrue(cache.Size() == 0);
			// Test cleaning inactive entry
			ret = cache.Put(new FileHandle(3), context3);
			NUnit.Framework.Assert.IsTrue(ret);
			ret = cache.Put(new FileHandle(4), context4);
			NUnit.Framework.Assert.IsTrue(ret);
			context3.SetActiveStatusForTest(false);
			cache.Scan(NfsConfigKeys.DfsNfsStreamTimeoutDefault);
			NUnit.Framework.Assert.IsTrue(cache.Size() == 1);
			NUnit.Framework.Assert.IsTrue(cache.Get(new FileHandle(3)) == null);
			NUnit.Framework.Assert.IsTrue(cache.Get(new FileHandle(4)) != null);
		}
	}
}
