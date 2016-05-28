using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSRename
	{
		internal static int CountLease(MiniDFSCluster cluster)
		{
			return NameNodeAdapter.GetLeaseManager(cluster.GetNamesystem()).CountLease();
		}

		internal readonly Path dir = new Path("/test/rename/");

		/// <exception cref="System.IO.IOException"/>
		internal virtual void List(FileSystem fs, string name)
		{
			FileSystem.Log.Info("\n\n" + name);
			foreach (FileStatus s in fs.ListStatus(dir))
			{
				FileSystem.Log.Info(string.Empty + s.GetPath());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void CreateFile(FileSystem fs, Path f)
		{
			DataOutputStream a_out = fs.Create(f);
			a_out.WriteBytes("something");
			a_out.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRename()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir));
				{
					//test lease
					Path a = new Path(dir, "a");
					Path aa = new Path(dir, "aa");
					Path b = new Path(dir, "b");
					CreateFile(fs, a);
					//should not have any lease
					NUnit.Framework.Assert.AreEqual(0, CountLease(cluster));
					DataOutputStream aa_out = fs.Create(aa);
					aa_out.WriteBytes("something");
					//should have 1 lease
					NUnit.Framework.Assert.AreEqual(1, CountLease(cluster));
					List(fs, "rename0");
					fs.Rename(a, b);
					List(fs, "rename1");
					aa_out.WriteBytes(" more");
					aa_out.Close();
					List(fs, "rename2");
					//should not have any lease
					NUnit.Framework.Assert.AreEqual(0, CountLease(cluster));
				}
				{
					// test non-existent destination
					Path dstPath = new Path("/c/d");
					NUnit.Framework.Assert.IsFalse(fs.Exists(dstPath));
					NUnit.Framework.Assert.IsFalse(fs.Rename(dir, dstPath));
				}
				{
					// dst cannot be a file or directory under src
					// test rename /a/b/foo to /a/b/c
					Path src = new Path("/a/b");
					Path dst = new Path("/a/b/c");
					CreateFile(fs, new Path(src, "foo"));
					// dst cannot be a file under src
					NUnit.Framework.Assert.IsFalse(fs.Rename(src, dst));
					// dst cannot be a directory under src
					NUnit.Framework.Assert.IsFalse(fs.Rename(src.GetParent(), dst.GetParent()));
				}
				{
					// dst can start with src, if it is not a directory or file under src
					// test rename /test /testfile
					Path src = new Path("/testPrefix");
					Path dst = new Path("/testPrefixfile");
					CreateFile(fs, src);
					NUnit.Framework.Assert.IsTrue(fs.Rename(src, dst));
				}
				{
					// dst should not be same as src test rename /a/b/c to /a/b/c
					Path src = new Path("/a/b/c");
					CreateFile(fs, src);
					NUnit.Framework.Assert.IsTrue(fs.Rename(src, src));
					NUnit.Framework.Assert.IsFalse(fs.Rename(new Path("/a/b"), new Path("/a/b/")));
					NUnit.Framework.Assert.IsTrue(fs.Rename(src, new Path("/a/b/c/")));
				}
				fs.Delete(dir, true);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Check the blocks of dst file are cleaned after rename with overwrite
		/// Restart NN to check the rename successfully
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRenameWithOverwrite()
		{
			short replFactor = 2;
			long blockSize = 512;
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(replFactor
				).Build();
			DistributedFileSystem dfs = cluster.GetFileSystem();
			try
			{
				long fileLen = blockSize * 3;
				string src = "/foo/src";
				string dst = "/foo/dst";
				Path srcPath = new Path(src);
				Path dstPath = new Path(dst);
				DFSTestUtil.CreateFile(dfs, srcPath, fileLen, replFactor, 1);
				DFSTestUtil.CreateFile(dfs, dstPath, fileLen, replFactor, 1);
				LocatedBlocks lbs = NameNodeAdapter.GetBlockLocations(cluster.GetNameNode(), dst, 
					0, fileLen);
				BlockManager bm = NameNodeAdapter.GetNamesystem(cluster.GetNameNode()).GetBlockManager
					();
				NUnit.Framework.Assert.IsTrue(bm.GetStoredBlock(lbs.GetLocatedBlocks()[0].GetBlock
					().GetLocalBlock()) != null);
				dfs.Rename(srcPath, dstPath, Options.Rename.Overwrite);
				NUnit.Framework.Assert.IsTrue(bm.GetStoredBlock(lbs.GetLocatedBlocks()[0].GetBlock
					().GetLocalBlock()) == null);
				// Restart NN and check the rename successfully
				cluster.RestartNameNodes();
				NUnit.Framework.Assert.IsFalse(dfs.Exists(srcPath));
				NUnit.Framework.Assert.IsTrue(dfs.Exists(dstPath));
			}
			finally
			{
				if (dfs != null)
				{
					dfs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}
	}
}
