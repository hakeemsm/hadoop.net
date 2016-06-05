using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>A JUnit test for checking if restarting DFS preserves integrity.</summary>
	public class TestRestartDFS
	{
		/// <exception cref="System.Exception"/>
		public virtual void RunTests(Configuration conf, bool serviceTest)
		{
			MiniDFSCluster cluster = null;
			DFSTestUtil files = new DFSTestUtil.Builder().SetName("TestRestartDFS").SetNumFiles
				(20).Build();
			string dir = "/srcdat";
			Path rootpath = new Path("/");
			Path dirpath = new Path(dir);
			long rootmtime;
			FileStatus rootstatus;
			FileStatus dirstatus;
			try
			{
				if (serviceTest)
				{
					conf.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, "localhost:0");
				}
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
				FileSystem fs = cluster.GetFileSystem();
				files.CreateFiles(fs, dir);
				rootmtime = fs.GetFileStatus(rootpath).GetModificationTime();
				rootstatus = fs.GetFileStatus(dirpath);
				dirstatus = fs.GetFileStatus(dirpath);
				fs.SetOwner(rootpath, rootstatus.GetOwner() + "_XXX", null);
				fs.SetOwner(dirpath, null, dirstatus.GetGroup() + "_XXX");
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			try
			{
				if (serviceTest)
				{
					conf.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, "localhost:0");
				}
				// Here we restart the MiniDFScluster without formatting namenode
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Format(false).Build();
				FileSystem fs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue("Filesystem corrupted after restart.", files.CheckFiles
					(fs, dir));
				FileStatus newrootstatus = fs.GetFileStatus(rootpath);
				NUnit.Framework.Assert.AreEqual(rootmtime, newrootstatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(rootstatus.GetOwner() + "_XXX", newrootstatus.GetOwner
					());
				NUnit.Framework.Assert.AreEqual(rootstatus.GetGroup(), newrootstatus.GetGroup());
				FileStatus newdirstatus = fs.GetFileStatus(dirpath);
				NUnit.Framework.Assert.AreEqual(dirstatus.GetOwner(), newdirstatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(dirstatus.GetGroup() + "_XXX", newdirstatus.GetGroup
					());
				rootmtime = fs.GetFileStatus(rootpath).GetModificationTime();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
			try
			{
				if (serviceTest)
				{
					conf.Set(DFSConfigKeys.DfsNamenodeServiceRpcAddressKey, "localhost:0");
				}
				// This is a second restart to check that after the first restart
				// the image written in parallel to both places did not get corrupted
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Format(false).Build();
				FileSystem fs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue("Filesystem corrupted after restart.", files.CheckFiles
					(fs, dir));
				FileStatus newrootstatus = fs.GetFileStatus(rootpath);
				NUnit.Framework.Assert.AreEqual(rootmtime, newrootstatus.GetModificationTime());
				NUnit.Framework.Assert.AreEqual(rootstatus.GetOwner() + "_XXX", newrootstatus.GetOwner
					());
				NUnit.Framework.Assert.AreEqual(rootstatus.GetGroup(), newrootstatus.GetGroup());
				FileStatus newdirstatus = fs.GetFileStatus(dirpath);
				NUnit.Framework.Assert.AreEqual(dirstatus.GetOwner(), newdirstatus.GetOwner());
				NUnit.Framework.Assert.AreEqual(dirstatus.GetGroup() + "_XXX", newdirstatus.GetGroup
					());
				files.Cleanup(fs, dir);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>check if DFS remains in proper condition after a restart</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRestartDFS()
		{
			Configuration conf = new HdfsConfiguration();
			RunTests(conf, false);
		}

		/// <summary>
		/// check if DFS remains in proper condition after a restart
		/// this rerun is with 2 ports enabled for RPC in the namenode
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRestartDualPortDFS()
		{
			Configuration conf = new HdfsConfiguration();
			RunTests(conf, true);
		}
	}
}
