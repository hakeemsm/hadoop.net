using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestMetadataVersionOutput
	{
		private MiniDFSCluster dfsCluster = null;

		private readonly Configuration conf = new Configuration();

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
			}
			Sharpen.Thread.Sleep(2000);
		}

		private void InitConfig()
		{
			conf.Set(DFSConfigKeys.DfsNameserviceId, "ns1");
			conf.Set(DFSConfigKeys.DfsHaNamenodesKeyPrefix + ".ns1", "nn1");
			conf.Set(DFSConfigKeys.DfsHaNamenodeIdKey, "nn1");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey + ".ns1.nn1", MiniDFSCluster.GetBaseDirectory
				() + "1");
			conf.Unset(DFSConfigKeys.DfsNamenodeNameDirKey);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestMetadataVersionOutput()
		{
			InitConfig();
			dfsCluster = new MiniDFSCluster.Builder(conf).ManageNameDfsDirs(false).NumDataNodes
				(1).CheckExitOnShutdown(false).Build();
			dfsCluster.WaitClusterUp();
			dfsCluster.Shutdown(false);
			InitConfig();
			TextWriter origOut = System.Console.Out;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			TextWriter stdOut = new TextWriter(baos);
			Runtime.SetOut(stdOut);
			try
			{
				NameNode.CreateNameNode(new string[] { "-metadataVersion" }, conf);
			}
			catch (Exception e)
			{
				GenericTestUtils.AssertExceptionContains("ExitException", e);
			}
			/* Check if meta data version is printed correctly. */
			string verNumStr = HdfsConstants.NamenodeLayoutVersion + string.Empty;
			NUnit.Framework.Assert.IsTrue(baos.ToString("UTF-8").Contains("HDFS Image Version: "
				 + verNumStr));
			NUnit.Framework.Assert.IsTrue(baos.ToString("UTF-8").Contains("Software format version: "
				 + verNumStr));
			Runtime.SetOut(origOut);
		}
	}
}
