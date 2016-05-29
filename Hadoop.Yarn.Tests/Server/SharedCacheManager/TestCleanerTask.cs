using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	public class TestCleanerTask
	{
		private const string Root = YarnConfiguration.DefaultSharedCacheRoot;

		private const long SleepTime = YarnConfiguration.DefaultScmCleanerResourceSleepMs;

		private const int NestedLevel = YarnConfiguration.DefaultSharedCacheNestedLevel;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonExistentRoot()
		{
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			CleanerMetrics metrics = Org.Mockito.Mockito.Mock<CleanerMetrics>();
			SCMStore store = Org.Mockito.Mockito.Mock<SCMStore>();
			CleanerTask task = CreateSpiedTask(fs, store, metrics, new ReentrantLock());
			// the shared cache root does not exist
			Org.Mockito.Mockito.When(fs.Exists(task.GetRootPath())).ThenReturn(false);
			task.Run();
			// process() should not be called
			Org.Mockito.Mockito.Verify(task, Org.Mockito.Mockito.Never()).Process();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProcessFreshResource()
		{
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			CleanerMetrics metrics = Org.Mockito.Mockito.Mock<CleanerMetrics>();
			SCMStore store = Org.Mockito.Mockito.Mock<SCMStore>();
			CleanerTask task = CreateSpiedTask(fs, store, metrics, new ReentrantLock());
			// mock a resource that is not evictable
			Org.Mockito.Mockito.When(store.IsResourceEvictable(Matchers.IsA<string>(), Matchers.IsA
				<FileStatus>())).ThenReturn(false);
			FileStatus status = Org.Mockito.Mockito.Mock<FileStatus>();
			Org.Mockito.Mockito.When(status.GetPath()).ThenReturn(new Path(Root + "/a/b/c/abc"
				));
			// process the resource
			task.ProcessSingleResource(status);
			// the directory should not be renamed
			Org.Mockito.Mockito.Verify(fs, Org.Mockito.Mockito.Never()).Rename(Matchers.Eq(status
				.GetPath()), Matchers.IsA<Path>());
			// metrics should record a processed file (but not delete)
			Org.Mockito.Mockito.Verify(metrics).ReportAFileProcess();
			Org.Mockito.Mockito.Verify(metrics, Org.Mockito.Mockito.Never()).ReportAFileDelete
				();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestProcessEvictableResource()
		{
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			CleanerMetrics metrics = Org.Mockito.Mockito.Mock<CleanerMetrics>();
			SCMStore store = Org.Mockito.Mockito.Mock<SCMStore>();
			CleanerTask task = CreateSpiedTask(fs, store, metrics, new ReentrantLock());
			// mock an evictable resource
			Org.Mockito.Mockito.When(store.IsResourceEvictable(Matchers.IsA<string>(), Matchers.IsA
				<FileStatus>())).ThenReturn(true);
			FileStatus status = Org.Mockito.Mockito.Mock<FileStatus>();
			Org.Mockito.Mockito.When(status.GetPath()).ThenReturn(new Path(Root + "/a/b/c/abc"
				));
			Org.Mockito.Mockito.When(store.RemoveResource(Matchers.IsA<string>())).ThenReturn
				(true);
			// rename succeeds
			Org.Mockito.Mockito.When(fs.Rename(Matchers.IsA<Path>(), Matchers.IsA<Path>())).ThenReturn
				(true);
			// delete returns true
			Org.Mockito.Mockito.When(fs.Delete(Matchers.IsA<Path>(), Matchers.AnyBoolean())).
				ThenReturn(true);
			// process the resource
			task.ProcessSingleResource(status);
			// the directory should be renamed
			Org.Mockito.Mockito.Verify(fs).Rename(Matchers.Eq(status.GetPath()), Matchers.IsA
				<Path>());
			// metrics should record a deleted file
			Org.Mockito.Mockito.Verify(metrics).ReportAFileDelete();
			Org.Mockito.Mockito.Verify(metrics, Org.Mockito.Mockito.Never()).ReportAFileProcess
				();
		}

		private CleanerTask CreateSpiedTask(FileSystem fs, SCMStore store, CleanerMetrics
			 metrics, Lock isCleanerRunning)
		{
			return Org.Mockito.Mockito.Spy(new CleanerTask(Root, SleepTime, NestedLevel, fs, 
				store, metrics, isCleanerRunning));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceIsInUseHasAnActiveApp()
		{
			FileSystem fs = Org.Mockito.Mockito.Mock<FileSystem>();
			CleanerMetrics metrics = Org.Mockito.Mockito.Mock<CleanerMetrics>();
			SCMStore store = Org.Mockito.Mockito.Mock<SCMStore>();
			FileStatus resource = Org.Mockito.Mockito.Mock<FileStatus>();
			Org.Mockito.Mockito.When(resource.GetPath()).ThenReturn(new Path(Root + "/a/b/c/abc"
				));
			// resource is stale
			Org.Mockito.Mockito.When(store.IsResourceEvictable(Matchers.IsA<string>(), Matchers.IsA
				<FileStatus>())).ThenReturn(true);
			// but still has appIds
			Org.Mockito.Mockito.When(store.RemoveResource(Matchers.IsA<string>())).ThenReturn
				(false);
			CleanerTask task = CreateSpiedTask(fs, store, metrics, new ReentrantLock());
			// process the resource
			task.ProcessSingleResource(resource);
			// metrics should record a processed file (but not delete)
			Org.Mockito.Mockito.Verify(metrics).ReportAFileProcess();
			Org.Mockito.Mockito.Verify(metrics, Org.Mockito.Mockito.Never()).ReportAFileDelete
				();
		}
	}
}
