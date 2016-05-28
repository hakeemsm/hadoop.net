using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Test the fileLength on cluster restarts</summary>
	public class TestFileLengthOnClusterRestart
	{
		/// <summary>
		/// Tests the fileLength when we sync the file and restart the cluster and
		/// Datanodes not report to Namenode yet.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFileLengthWithHSyncAndClusterRestartWithOutDNsRegister()
		{
			Configuration conf = new HdfsConfiguration();
			// create cluster
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, 512);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			HdfsDataInputStream @in = null;
			try
			{
				Path path = new Path("/tmp/TestFileLengthOnClusterRestart", "test");
				DistributedFileSystem dfs = cluster.GetFileSystem();
				FSDataOutputStream @out = dfs.Create(path);
				int fileLength = 1030;
				@out.Write(new byte[fileLength]);
				@out.Hsync();
				cluster.RestartNameNode();
				cluster.WaitActive();
				@in = (HdfsDataInputStream)dfs.Open(path, 1024);
				// Verify the length when we just restart NN. DNs will register
				// immediately.
				NUnit.Framework.Assert.AreEqual(fileLength, @in.GetVisibleLength());
				cluster.ShutdownDataNodes();
				cluster.RestartNameNode(false);
				// This is just for ensuring NN started.
				VerifyNNIsInSafeMode(dfs);
				try
				{
					@in = (HdfsDataInputStream)dfs.Open(path);
					NUnit.Framework.Assert.Fail("Expected IOException");
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue(e.GetLocalizedMessage().IndexOf("Name node is in safe mode"
						) >= 0);
				}
			}
			finally
			{
				if (null != @in)
				{
					@in.Close();
				}
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyNNIsInSafeMode(DistributedFileSystem dfs)
		{
			while (true)
			{
				try
				{
					if (dfs.IsInSafeMode())
					{
						return;
					}
					else
					{
						throw new IOException("Expected to be in SafeMode");
					}
				}
				catch (IOException)
				{
				}
			}
		}
		// NN might not started completely Ignore
	}
}
