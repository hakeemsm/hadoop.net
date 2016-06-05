using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A JUnit test to test Mini Map-Reduce Cluster with Mini-DFS.</summary>
	public class TestMiniMRWithDFSWithDistinctUsers
	{
		internal static readonly UserGroupInformation DfsUgi = CreateUGI("dfs", true);

		internal static readonly UserGroupInformation AliceUgi = CreateUGI("alice", false
			);

		internal static readonly UserGroupInformation BobUgi = CreateUGI("bob", false);

		internal MiniMRCluster mr = null;

		internal MiniDFSCluster dfs = null;

		internal FileSystem fs = null;

		internal Configuration conf = new Configuration();

		internal static UserGroupInformation CreateUGI(string name, bool issuper)
		{
			string group = issuper ? "supergroup" : name;
			return UserGroupInformation.CreateUserForTesting(name, new string[] { group });
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void Mkdir(FileSystem fs, string dir, string user, string group, 
			short mode)
		{
			Path p = new Path(dir);
			fs.Mkdirs(p);
			fs.SetPermission(p, new FsPermission(mode));
			fs.SetOwner(p, user, group);
		}

		// runs a sample job as a user (ugi)
		/// <exception cref="System.Exception"/>
		internal virtual void RunJobAsUser(JobConf job, UserGroupInformation ugi)
		{
			RunningJob rj = ugi.DoAs(new _PrivilegedExceptionAction_66(job));
			rj.WaitForCompletion();
			NUnit.Framework.Assert.AreEqual("SUCCEEDED", JobStatus.GetJobRunState(rj.GetJobState
				()));
		}

		private sealed class _PrivilegedExceptionAction_66 : PrivilegedExceptionAction<RunningJob
			>
		{
			public _PrivilegedExceptionAction_66(JobConf job)
			{
				this.job = job;
			}

			/// <exception cref="System.IO.IOException"/>
			public RunningJob Run()
			{
				return JobClient.RunJob(job);
			}

			private readonly JobConf job;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			dfs = new MiniDFSCluster.Builder(conf).NumDataNodes(4).Build();
			fs = DfsUgi.DoAs(new _PrivilegedExceptionAction_80(this));
			// Home directories for users
			Mkdir(fs, "/user", "nobody", "nogroup", (short)0x3ff);
			Mkdir(fs, "/user/alice", "alice", "nogroup", (short)0x1ed);
			Mkdir(fs, "/user/bob", "bob", "nogroup", (short)0x1ed);
			// staging directory root with sticky bit
			UserGroupInformation MrUgi = UserGroupInformation.GetLoginUser();
			Mkdir(fs, "/staging", MrUgi.GetShortUserName(), "nogroup", (short)0x3ff);
			JobConf mrConf = new JobConf();
			mrConf.Set(JTConfig.JtStagingAreaRoot, "/staging");
			mr = new MiniMRCluster(0, 0, 4, dfs.GetFileSystem().GetUri().ToString(), 1, null, 
				null, MrUgi, mrConf);
		}

		private sealed class _PrivilegedExceptionAction_80 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_80(TestMiniMRWithDFSWithDistinctUsers _enclosing
				)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public FileSystem Run()
			{
				return this._enclosing.dfs.GetFileSystem();
			}

			private readonly TestMiniMRWithDFSWithDistinctUsers _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (mr != null)
			{
				mr.Shutdown();
			}
			if (dfs != null)
			{
				dfs.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDistinctUsers()
		{
			JobConf job1 = mr.CreateJobConf();
			string input = "The quick brown fox\nhas many silly\n" + "red fox sox\n";
			Path inDir = new Path("/testing/distinct/input");
			Path outDir = new Path("/user/alice/output");
			TestMiniMRClasspath.ConfigureWordCount(fs, job1, input, 2, 1, inDir, outDir);
			RunJobAsUser(job1, AliceUgi);
			JobConf job2 = mr.CreateJobConf();
			Path inDir2 = new Path("/testing/distinct/input2");
			Path outDir2 = new Path("/user/bob/output2");
			TestMiniMRClasspath.ConfigureWordCount(fs, job2, input, 2, 1, inDir2, outDir2);
			RunJobAsUser(job2, BobUgi);
		}

		/// <summary>Regression test for MAPREDUCE-2327.</summary>
		/// <remarks>
		/// Regression test for MAPREDUCE-2327. Verifies that, even if a map
		/// task makes lots of spills (more than fit in the spill index cache)
		/// that it will succeed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleSpills()
		{
			JobConf job1 = mr.CreateJobConf();
			// Make sure it spills twice
			job1.SetFloat(MRJobConfig.MapSortSpillPercent, 0.0001f);
			job1.SetInt(MRJobConfig.IoSortMb, 1);
			// Make sure the spill records don't fit in index cache
			job1.SetInt(MRJobConfig.IndexCacheMemoryLimit, 0);
			string input = "The quick brown fox\nhas many silly\n" + "red fox sox\n";
			Path inDir = new Path("/testing/distinct/input");
			Path outDir = new Path("/user/alice/output");
			TestMiniMRClasspath.ConfigureWordCount(fs, job1, input, 2, 1, inDir, outDir);
			RunJobAsUser(job1, AliceUgi);
		}
	}
}
