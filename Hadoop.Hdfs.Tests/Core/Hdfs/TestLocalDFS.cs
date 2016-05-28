using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This class tests the DFS class via the FileSystem interface in a single node
	/// mini-cluster.
	/// </summary>
	public class TestLocalDFS
	{
		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name)
		{
			DataOutputStream stm = fileSys.Create(name);
			stm.WriteBytes("oom");
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void ReadFile(FileSystem fileSys, Path name)
		{
			DataInputStream stm = fileSys.Open(name);
			byte[] buffer = new byte[4];
			int bytesRead = stm.Read(buffer, 0, 4);
			NUnit.Framework.Assert.AreEqual("oom", Sharpen.Runtime.GetStringForBytes(buffer, 
				0, bytesRead));
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		internal static string GetUserName(FileSystem fs)
		{
			if (fs is DistributedFileSystem)
			{
				return ((DistributedFileSystem)fs).dfs.ugi.GetShortUserName();
			}
			return Runtime.GetProperty("user.name");
		}

		/// <summary>Tests get/set working directory in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWorkingDirectory()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fileSys = cluster.GetFileSystem();
			try
			{
				Path orig_path = fileSys.GetWorkingDirectory();
				NUnit.Framework.Assert.IsTrue(orig_path.IsAbsolute());
				Path file1 = new Path("somewhat/random.txt");
				WriteFile(fileSys, file1);
				NUnit.Framework.Assert.IsTrue(fileSys.Exists(new Path(orig_path, file1.ToString()
					)));
				fileSys.Delete(file1, true);
				Path subdir1 = new Path("/somewhere");
				fileSys.SetWorkingDirectory(subdir1);
				WriteFile(fileSys, file1);
				CleanupFile(fileSys, new Path(subdir1, file1.ToString()));
				Path subdir2 = new Path("else");
				fileSys.SetWorkingDirectory(subdir2);
				WriteFile(fileSys, file1);
				ReadFile(fileSys, file1);
				CleanupFile(fileSys, new Path(new Path(subdir1, subdir2.ToString()), file1.ToString
					()));
				// test home directory
				Path home = fileSys.MakeQualified(new Path(DFSConfigKeys.DfsUserHomeDirPrefixDefault
					 + "/" + GetUserName(fileSys)));
				Path fsHome = fileSys.GetHomeDirectory();
				NUnit.Framework.Assert.AreEqual(home, fsHome);
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>Tests get/set working directory in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHomeDirectory()
		{
			string[] homeBases = new string[] { "/home", "/home/user" };
			Configuration conf = new HdfsConfiguration();
			foreach (string homeBase in homeBases)
			{
				conf.Set(DFSConfigKeys.DfsUserHomeDirPrefixKey, homeBase);
				MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
				FileSystem fileSys = cluster.GetFileSystem();
				try
				{
					// test home directory
					Path home = fileSys.MakeQualified(new Path(homeBase + "/" + GetUserName(fileSys))
						);
					Path fsHome = fileSys.GetHomeDirectory();
					NUnit.Framework.Assert.AreEqual(home, fsHome);
				}
				finally
				{
					fileSys.Close();
					cluster.Shutdown();
				}
			}
		}
	}
}
