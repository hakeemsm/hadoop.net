using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Qjournal;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	public class TestDFSAdminWithHA
	{
		private readonly ByteArrayOutputStream @out = new ByteArrayOutputStream();

		private readonly ByteArrayOutputStream err = new ByteArrayOutputStream();

		private MiniQJMHACluster cluster;

		private Configuration conf;

		private DFSAdmin admin;

		private TextWriter originOut;

		private TextWriter originErr;

		private const string Nsid = "ns1";

		private static string newLine = Runtime.GetProperty("line.separator");

		private void AssertOutputMatches(string @string)
		{
			string errOutput = new string(@out.ToByteArray(), Charsets.Utf8);
			string output = new string(@out.ToByteArray(), Charsets.Utf8);
			if (!errOutput.Matches(@string) && !output.Matches(@string))
			{
				NUnit.Framework.Assert.Fail("Expected output to match '" + @string + "' but err_output was:\n"
					 + errOutput + "\n and output was: \n" + output);
			}
			@out.Reset();
			err.Reset();
		}

		private void SetHAConf(Configuration conf, string nn1Addr, string nn2Addr)
		{
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, "hdfs://" + Nsid);
			conf.Set(DFSConfigKeys.DfsNameservices, Nsid);
			conf.Set(DFSConfigKeys.DfsNameserviceId, Nsid);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsHaNamenodesKeyPrefix, Nsid), "nn1,nn2"
				);
			conf.Set(DFSConfigKeys.DfsHaNamenodeIdKey, "nn1");
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, Nsid, "nn1"
				), nn1Addr);
			conf.Set(DFSUtil.AddKeySuffixes(DFSConfigKeys.DfsNamenodeRpcAddressKey, Nsid, "nn2"
				), nn2Addr);
		}

		/// <exception cref="System.Exception"/>
		private void SetUpHaCluster(bool security)
		{
			conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, security);
			cluster = new MiniQJMHACluster.Builder(conf).Build();
			SetHAConf(conf, cluster.GetDfsCluster().GetNameNode(0).GetHostAndPort(), cluster.
				GetDfsCluster().GetNameNode(1).GetHostAndPort());
			cluster.GetDfsCluster().GetNameNode(0).GetHostAndPort();
			admin = new DFSAdmin();
			admin.SetConf(conf);
			NUnit.Framework.Assert.IsTrue(HAUtil.IsHAEnabled(conf, "ns1"));
			originOut = System.Console.Out;
			originErr = System.Console.Error;
			Runtime.SetOut(new TextWriter(@out));
			Runtime.SetErr(new TextWriter(err));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			System.Console.Out.Flush();
			System.Console.Error.Flush();
			Runtime.SetOut(originOut);
			Runtime.SetErr(originErr);
			if (admin != null)
			{
				admin.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
			@out.Reset();
			err.Reset();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetSafeMode()
		{
			SetUpHaCluster(false);
			// Enter safemode
			int exitCode = admin.Run(new string[] { "-safemode", "enter" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Safe mode is ON in.*";
			AssertOutputMatches(message + newLine + message + newLine);
			// Get safemode
			exitCode = admin.Run(new string[] { "-safemode", "get" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			message = "Safe mode is ON in.*";
			AssertOutputMatches(message + newLine + message + newLine);
			// Leave safemode
			exitCode = admin.Run(new string[] { "-safemode", "leave" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			message = "Safe mode is OFF in.*";
			AssertOutputMatches(message + newLine + message + newLine);
			// Get safemode
			exitCode = admin.Run(new string[] { "-safemode", "get" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			message = "Safe mode is OFF in.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSaveNamespace()
		{
			SetUpHaCluster(false);
			// Safe mode should be turned ON in order to create namespace image.
			int exitCode = admin.Run(new string[] { "-safemode", "enter" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Safe mode is ON in.*";
			AssertOutputMatches(message + newLine + message + newLine);
			exitCode = admin.Run(new string[] { "-saveNamespace" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			message = "Save namespace successful for.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRestoreFailedStorage()
		{
			SetUpHaCluster(false);
			int exitCode = admin.Run(new string[] { "-restoreFailedStorage", "check" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "restoreFailedStorage is set to false for.*";
			// Default is false
			AssertOutputMatches(message + newLine + message + newLine);
			exitCode = admin.Run(new string[] { "-restoreFailedStorage", "true" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			message = "restoreFailedStorage is set to true for.*";
			AssertOutputMatches(message + newLine + message + newLine);
			exitCode = admin.Run(new string[] { "-restoreFailedStorage", "false" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			message = "restoreFailedStorage is set to false for.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshNodes()
		{
			SetUpHaCluster(false);
			int exitCode = admin.Run(new string[] { "-refreshNodes" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Refresh nodes successful for.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSetBalancerBandwidth()
		{
			SetUpHaCluster(false);
			int exitCode = admin.Run(new string[] { "-setBalancerBandwidth", "10" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Balancer bandwidth is set to 10 for.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMetaSave()
		{
			SetUpHaCluster(false);
			int exitCode = admin.Run(new string[] { "-metasave", "dfs.meta" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Created metasave file dfs.meta in the log directory" + " of namenode.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshServiceAcl()
		{
			SetUpHaCluster(true);
			int exitCode = admin.Run(new string[] { "-refreshServiceAcl" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Refresh service acl successful for.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshUserToGroupsMappings()
		{
			SetUpHaCluster(false);
			int exitCode = admin.Run(new string[] { "-refreshUserToGroupsMappings" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Refresh user to groups mapping successful for.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshSuperUserGroupsConfiguration()
		{
			SetUpHaCluster(false);
			int exitCode = admin.Run(new string[] { "-refreshSuperUserGroupsConfiguration" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Refresh super user groups configuration successful for.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefreshCallQueue()
		{
			SetUpHaCluster(false);
			int exitCode = admin.Run(new string[] { "-refreshCallQueue" });
			NUnit.Framework.Assert.AreEqual(err.ToString().Trim(), 0, exitCode);
			string message = "Refresh call queue successful for.*";
			AssertOutputMatches(message + newLine + message + newLine);
		}
	}
}
