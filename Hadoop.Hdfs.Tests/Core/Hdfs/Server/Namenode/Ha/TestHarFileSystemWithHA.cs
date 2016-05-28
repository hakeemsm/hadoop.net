using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestHarFileSystemWithHA
	{
		private static readonly Path TestHarPath = new Path("/input.har");

		/// <summary>
		/// Test that the HarFileSystem works with underlying HDFS URIs that have no
		/// port specified, as is often the case with an HA setup.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHarUriWithHaUriWithNoPort()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).NnTopology(MiniDFSNNTopology
					.SimpleHATopology()).Build();
				cluster.TransitionToActive(0);
				HATestUtil.SetFailoverConfigurations(cluster, conf);
				CreateEmptyHarArchive(HATestUtil.ConfigureFailoverFs(cluster, conf), TestHarPath);
				URI failoverUri = FileSystem.GetDefaultUri(conf);
				Path p = new Path("har://hdfs-" + failoverUri.GetAuthority() + TestHarPath);
				p.GetFileSystem(conf);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Create an empty Har archive in the FileSystem fs at the Path p.</summary>
		/// <param name="fs">the file system to create the Har archive in</param>
		/// <param name="p">the path to create the Har archive at</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		private static void CreateEmptyHarArchive(FileSystem fs, Path p)
		{
			fs.Mkdirs(p);
			OutputStream @out = fs.Create(new Path(p, "_masterindex"));
			@out.Write(Sharpen.Runtime.GetBytesForString(Sharpen.Extensions.ToString(HarFileSystem
				.Version)));
			@out.Close();
			fs.Create(new Path(p, "_index")).Close();
		}
	}
}
