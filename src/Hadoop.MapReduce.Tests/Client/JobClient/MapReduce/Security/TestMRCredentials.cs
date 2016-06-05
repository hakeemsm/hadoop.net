using System;
using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security
{
	/// <summary>
	/// Tests whether a protected secret passed from JobClient is
	/// available to the child task
	/// </summary>
	public class TestMRCredentials
	{
		internal const int NumOfKeys = 10;

		private static MiniMRClientCluster mrCluster;

		private static MiniDFSCluster dfsCluster;

		private static int numSlaves = 1;

		private static JobConf jConf;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			Runtime.SetProperty("hadoop.log.dir", "logs");
			Configuration conf = new Configuration();
			dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numSlaves).Build();
			jConf = new JobConf(conf);
			FileSystem.SetDefaultUri(conf, dfsCluster.GetFileSystem().GetUri().ToString());
			mrCluster = MiniMRClientClusterFactory.Create(typeof(TestMRCredentials), 1, jConf
				);
			CreateKeysAsJson("keys.json");
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (mrCluster != null)
			{
				mrCluster.Stop();
			}
			mrCluster = null;
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
			}
			dfsCluster = null;
			new FilePath("keys.json").Delete();
		}

		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.IO.IOException"/>
		public static void CreateKeysAsJson(string fileName)
		{
			StringBuilder jsonString = new StringBuilder();
			jsonString.Append("{");
			for (int i = 0; i < NumOfKeys; i++)
			{
				string keyName = "alias" + i;
				string password = "password" + i;
				jsonString.Append("\"" + keyName + "\":" + "\"" + password + "\"");
				if (i < (NumOfKeys - 1))
				{
					jsonString.Append(",");
				}
			}
			jsonString.Append("}");
			FileOutputStream fos = new FileOutputStream(fileName);
			fos.Write(Sharpen.Runtime.GetBytesForString(jsonString.ToString()));
			fos.Close();
		}

		/// <summary>run a distributed job and verify that TokenCache is available</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void Test()
		{
			// make sure JT starts
			Configuration jobConf = new JobConf(mrCluster.GetConfig());
			// provide namenodes names for the job to get the delegation tokens for
			//String nnUri = dfsCluster.getNameNode().getUri(namenode).toString();
			NameNode nn = dfsCluster.GetNameNode();
			URI nnUri = NameNode.GetUri(nn.GetNameNodeAddress());
			jobConf.Set(JobContext.JobNamenodes, nnUri + "," + nnUri.ToString());
			jobConf.Set("mapreduce.job.credentials.json", "keys.json");
			// using argument to pass the file name
			string[] args = new string[] { "-m", "1", "-r", "1", "-mt", "1", "-rt", "1" };
			int res = -1;
			try
			{
				res = ToolRunner.Run(jobConf, new CredentialsTestJob(), args);
			}
			catch (Exception e)
			{
				System.Console.Out.WriteLine("Job failed with" + e.GetLocalizedMessage());
				Sharpen.Runtime.PrintStackTrace(e, System.Console.Out);
				NUnit.Framework.Assert.Fail("Job failed");
			}
			NUnit.Framework.Assert.AreEqual("dist job res is not 0", res, 0);
		}
	}
}
