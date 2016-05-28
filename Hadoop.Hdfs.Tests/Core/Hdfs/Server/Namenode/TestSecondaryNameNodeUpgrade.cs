using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Regression test for HDFS-3597, SecondaryNameNode upgrade -- when a 2NN
	/// starts up with an existing directory structure with an old VERSION file, it
	/// should delete the snapshot and download a new one from the NN.
	/// </summary>
	public class TestSecondaryNameNodeUpgrade
	{
		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void CleanupCluster()
		{
			FilePath hdfsDir = new FilePath(MiniDFSCluster.GetBaseDirectory()).GetCanonicalFile
				();
			System.Console.Out.WriteLine("cleanupCluster deleting " + hdfsDir);
			if (hdfsDir.Exists() && !FileUtil.FullyDelete(hdfsDir))
			{
				throw new IOException("Could not delete hdfs directory '" + hdfsDir + "'");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoIt(IDictionary<string, string> paramsToCorrupt)
		{
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			SecondaryNameNode snn = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				conf.Set(DFSConfigKeys.DfsNamenodeSecondaryHttpAddressKey, "0.0.0.0:0");
				snn = new SecondaryNameNode(conf);
				fs = cluster.GetFileSystem();
				fs.Mkdirs(new Path("/test/foo"));
				snn.DoCheckpoint();
				IList<FilePath> versionFiles = snn.GetFSImage().GetStorage().GetFiles(null, "VERSION"
					);
				snn.Shutdown();
				foreach (FilePath versionFile in versionFiles)
				{
					foreach (KeyValuePair<string, string> paramToCorrupt in paramsToCorrupt)
					{
						string param = paramToCorrupt.Key;
						string val = paramToCorrupt.Value;
						System.Console.Out.WriteLine("Changing '" + param + "' to '" + val + "' in " + versionFile
							);
						FSImageTestUtil.CorruptVersionFile(versionFile, param, val);
					}
				}
				snn = new SecondaryNameNode(conf);
				fs.Mkdirs(new Path("/test/bar"));
				snn.DoCheckpoint();
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
				if (snn != null)
				{
					snn.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradeLayoutVersionSucceeds()
		{
			DoIt(ImmutableMap.Of("layoutVersion", "-39"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUpgradePreFedSucceeds()
		{
			DoIt(ImmutableMap.Of("layoutVersion", "-19", "clusterID", string.Empty, "blockpoolID"
				, string.Empty));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestChangeNsIDFails()
		{
			try
			{
				DoIt(ImmutableMap.Of("namespaceID", "2"));
				NUnit.Framework.Assert.Fail("Should throw InconsistentFSStateException");
			}
			catch (IOException e)
			{
				GenericTestUtils.AssertExceptionContains("Inconsistent checkpoint fields", e);
				System.Console.Out.WriteLine("Correctly failed with inconsistent namespaceID: " +
					 e);
			}
		}
	}
}
