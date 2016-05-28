using System.IO;
using System.Text;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Regression test for HDFS-1542, a deadlock between the main thread
	/// and the DFSOutputStream.DataStreamer thread caused because
	/// Configuration.writeXML holds a lock on itself while writing to DFS.
	/// </summary>
	public class TestWriteConfigurationToDFS
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestWriteConf()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, 4096);
			System.Console.Out.WriteLine("Setting conf in: " + Runtime.IdentityHashCode(conf)
				);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			FileSystem fs = null;
			OutputStream os = null;
			try
			{
				fs = cluster.GetFileSystem();
				Path filePath = new Path("/testWriteConf.xml");
				os = fs.Create(filePath);
				StringBuilder longString = new StringBuilder();
				for (int i = 0; i < 100000; i++)
				{
					longString.Append("hello");
				}
				// 500KB
				conf.Set("foobar", longString.ToString());
				conf.WriteXml(os);
				os.Close();
				os = null;
				fs.Close();
				fs = null;
			}
			finally
			{
				IOUtils.Cleanup(null, os, fs);
				cluster.Shutdown();
			}
		}
	}
}
