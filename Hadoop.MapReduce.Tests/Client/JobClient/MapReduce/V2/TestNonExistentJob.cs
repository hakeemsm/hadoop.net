using System.Net;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2
{
	public class TestNonExistentJob : TestCase
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
		public virtual void TestGetInvalidJob()
		{
			RunningJob runJob = new JobClient(GetJobConf()).GetJob(((JobID)JobID.ForName("job_0_0"
				)));
			NUnit.Framework.Assert.IsNull(runJob);
		}
	}
}
