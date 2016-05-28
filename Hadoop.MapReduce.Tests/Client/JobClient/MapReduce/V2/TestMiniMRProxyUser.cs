using System;
using System.IO;
using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestMiniMRProxyUser : TestCase
	{
		private MiniDFSCluster dfsCluster = null;

		private MiniMRCluster mrCluster = null;

		/// <exception cref="System.Exception"/>
		protected override void SetUp()
		{
			base.SetUp();
			if (Runtime.GetProperty("hadoop.log.dir") == null)
			{
				Runtime.SetProperty("hadoop.log.dir", "/tmp");
			}
			int taskTrackers = 2;
			int dataNodes = 2;
			string proxyUser = Runtime.GetProperty("user.name");
			string proxyGroup = "g";
			StringBuilder sb = new StringBuilder();
			sb.Append("127.0.0.1,localhost");
			foreach (IPAddress i in IPAddress.GetAllByName(Sharpen.Runtime.GetLocalHost().GetHostName
				()))
			{
				sb.Append(",").Append(i.ToString());
			}
			JobConf conf = new JobConf();
			conf.Set("dfs.block.access.token.enable", "false");
			conf.Set("dfs.permissions", "true");
			conf.Set("hadoop.security.authentication", "simple");
			conf.Set("hadoop.proxyuser." + proxyUser + ".hosts", sb.ToString());
			conf.Set("hadoop.proxyuser." + proxyUser + ".groups", proxyGroup);
			string[] userGroups = new string[] { proxyGroup };
			UserGroupInformation.CreateUserForTesting(proxyUser, userGroups);
			UserGroupInformation.CreateUserForTesting("u1", userGroups);
			UserGroupInformation.CreateUserForTesting("u2", new string[] { "gg" });
			dfsCluster = new MiniDFSCluster.Builder(conf).NumDataNodes(dataNodes).Build();
			FileSystem fileSystem = dfsCluster.GetFileSystem();
			fileSystem.Mkdirs(new Path("/tmp"));
			fileSystem.Mkdirs(new Path("/user"));
			fileSystem.Mkdirs(new Path("/hadoop/mapred/system"));
			fileSystem.SetPermission(new Path("/tmp"), FsPermission.ValueOf("-rwxrwxrwx"));
			fileSystem.SetPermission(new Path("/user"), FsPermission.ValueOf("-rwxrwxrwx"));
			fileSystem.SetPermission(new Path("/hadoop/mapred/system"), FsPermission.ValueOf(
				"-rwx------"));
			string nnURI = fileSystem.GetUri().ToString();
			int numDirs = 1;
			string[] racks = null;
			string[] hosts = null;
			mrCluster = new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs, racks, hosts, null
				, conf);
			ProxyUsers.RefreshSuperUserGroupsConfiguration(conf);
		}

		protected internal virtual JobConf GetJobConf()
		{
			return mrCluster.CreateJobConf();
		}

		/// <exception cref="System.Exception"/>
		protected override void TearDown()
		{
			if (mrCluster != null)
			{
				mrCluster.Shutdown();
			}
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
			}
			base.TearDown();
		}

		/// <exception cref="System.Exception"/>
		private void MrRun()
		{
			FileSystem fs = FileSystem.Get(GetJobConf());
			Path inputDir = new Path("input");
			fs.Mkdirs(inputDir);
			TextWriter writer = new OutputStreamWriter(fs.Create(new Path(inputDir, "data.txt"
				)));
			writer.Write("hello");
			writer.Close();
			Path outputDir = new Path("output", "output");
			JobConf jobConf = new JobConf(GetJobConf());
			jobConf.SetInt("mapred.map.tasks", 1);
			jobConf.SetInt("mapred.map.max.attempts", 1);
			jobConf.SetInt("mapred.reduce.max.attempts", 1);
			jobConf.Set("mapred.input.dir", inputDir.ToString());
			jobConf.Set("mapred.output.dir", outputDir.ToString());
			JobClient jobClient = new JobClient(jobConf);
			RunningJob runJob = jobClient.SubmitJob(jobConf);
			runJob.WaitForCompletion();
			NUnit.Framework.Assert.IsTrue(runJob.IsComplete());
			NUnit.Framework.Assert.IsTrue(runJob.IsSuccessful());
		}

		/// <exception cref="System.Exception"/>
		public virtual void __testCurrentUser()
		{
			MrRun();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestValidProxyUser()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateProxyUser("u1", UserGroupInformation
				.GetLoginUser());
			ugi.DoAs(new _PrivilegedExceptionAction_135(this));
		}

		private sealed class _PrivilegedExceptionAction_135 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_135(TestMiniMRProxyUser _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.MrRun();
				return null;
			}

			private readonly TestMiniMRProxyUser _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void ___testInvalidProxyUser()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateProxyUser("u2", UserGroupInformation
				.GetLoginUser());
			ugi.DoAs(new _PrivilegedExceptionAction_147(this));
		}

		private sealed class _PrivilegedExceptionAction_147 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_147(TestMiniMRProxyUser _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				try
				{
					this._enclosing.MrRun();
					TestCase.Fail();
				}
				catch (RemoteException)
				{
				}
				catch (Exception)
				{
					//nop
					TestCase.Fail();
				}
				return null;
			}

			private readonly TestMiniMRProxyUser _enclosing;
		}
	}
}
