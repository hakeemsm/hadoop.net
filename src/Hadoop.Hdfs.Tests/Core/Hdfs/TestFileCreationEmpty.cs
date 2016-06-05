using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Test empty file creation.</summary>
	public class TestFileCreationEmpty
	{
		private bool isConcurrentModificationException = false;

		/// <summary>This test creates three empty files and lets their leases expire.</summary>
		/// <remarks>
		/// This test creates three empty files and lets their leases expire.
		/// This triggers release of the leases.
		/// The empty files are supposed to be closed by that
		/// without causing ConcurrentModificationException.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLeaseExpireEmptyFiles()
		{
			Sharpen.Thread.UncaughtExceptionHandler oldUEH = Sharpen.Thread.GetDefaultUncaughtExceptionHandler
				();
			Sharpen.Thread.SetDefaultUncaughtExceptionHandler(new _UncaughtExceptionHandler_43
				(this));
			System.Console.Out.WriteLine("testLeaseExpireEmptyFiles start");
			long leasePeriod = 1000;
			int DatanodeNum = 3;
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1000);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			// create cluster
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(DatanodeNum
				).Build();
			try
			{
				cluster.WaitActive();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				// create a new file.
				TestFileCreation.CreateFile(dfs, new Path("/foo"), DatanodeNum);
				TestFileCreation.CreateFile(dfs, new Path("/foo2"), DatanodeNum);
				TestFileCreation.CreateFile(dfs, new Path("/foo3"), DatanodeNum);
				// set the soft and hard limit to be 1 second so that the
				// namenode triggers lease recovery
				cluster.SetLeasePeriod(leasePeriod, leasePeriod);
				// wait for the lease to expire
				try
				{
					Sharpen.Thread.Sleep(5 * leasePeriod);
				}
				catch (Exception)
				{
				}
				NUnit.Framework.Assert.IsFalse(isConcurrentModificationException);
			}
			finally
			{
				Sharpen.Thread.SetDefaultUncaughtExceptionHandler(oldUEH);
				cluster.Shutdown();
			}
		}

		private sealed class _UncaughtExceptionHandler_43 : Sharpen.Thread.UncaughtExceptionHandler
		{
			public _UncaughtExceptionHandler_43(TestFileCreationEmpty _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public void UncaughtException(Sharpen.Thread t, Exception e)
			{
				if (e is ConcurrentModificationException)
				{
					LeaseManager.Log.Error("t=" + t, e);
					this._enclosing.isConcurrentModificationException = true;
				}
			}

			private readonly TestFileCreationEmpty _enclosing;
		}
	}
}
