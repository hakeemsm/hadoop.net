using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security.Ssl;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Security.Ssl
{
	public class TestEncryptedShuffle
	{
		private static readonly string Basedir = Runtime.GetProperty("test.build.dir", "target/test-dir"
			) + "/" + typeof(TestEncryptedShuffle).Name;

		private string classpathDir;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUp()
		{
			FilePath @base = new FilePath(Basedir);
			FileUtil.FullyDelete(@base);
			@base.Mkdirs();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void CreateCustomYarnClasspath()
		{
			classpathDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestEncryptedShuffle));
			new FilePath(classpathDir, "core-site.xml").Delete();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void CleanUpMiniClusterSpecialConfig()
		{
			new FilePath(classpathDir, "core-site.xml").Delete();
			string keystoresDir = new FilePath(Basedir).GetAbsolutePath();
			KeyStoreTestUtil.CleanupSSLConfig(keystoresDir, classpathDir);
		}

		private MiniDFSCluster dfsCluster = null;

		private MiniMRClientCluster mrCluster = null;

		/// <exception cref="System.Exception"/>
		private void StartCluster(Configuration conf)
		{
			if (Runtime.GetProperty("hadoop.log.dir") == null)
			{
				Runtime.SetProperty("hadoop.log.dir", "target/test-dir");
			}
			conf.Set("dfs.block.access.token.enable", "false");
			conf.Set("dfs.permissions", "true");
			conf.Set("hadoop.security.authentication", "simple");
			string cp = conf.Get(YarnConfiguration.YarnApplicationClasspath, StringUtils.Join
				(",", YarnConfiguration.DefaultYarnCrossPlatformApplicationClasspath)) + FilePath
				.pathSeparator + classpathDir;
			conf.Set(YarnConfiguration.YarnApplicationClasspath, cp);
			dfsCluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fileSystem = dfsCluster.GetFileSystem();
			fileSystem.Mkdirs(new Path("/tmp"));
			fileSystem.Mkdirs(new Path("/user"));
			fileSystem.Mkdirs(new Path("/hadoop/mapred/system"));
			fileSystem.SetPermission(new Path("/tmp"), FsPermission.ValueOf("-rwxrwxrwx"));
			fileSystem.SetPermission(new Path("/user"), FsPermission.ValueOf("-rwxrwxrwx"));
			fileSystem.SetPermission(new Path("/hadoop/mapred/system"), FsPermission.ValueOf(
				"-rwx------"));
			FileSystem.SetDefaultUri(conf, fileSystem.GetUri());
			mrCluster = MiniMRClientClusterFactory.Create(this.GetType(), 1, conf);
			// so the minicluster conf is avail to the containers.
			TextWriter writer = new FileWriter(classpathDir + "/core-site.xml");
			mrCluster.GetConfig().WriteXml(writer);
			writer.Close();
		}

		/// <exception cref="System.Exception"/>
		private void StopCluster()
		{
			if (mrCluster != null)
			{
				mrCluster.Stop();
			}
			if (dfsCluster != null)
			{
				dfsCluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual JobConf GetJobConf()
		{
			return new JobConf(mrCluster.GetConfig());
		}

		/// <exception cref="System.Exception"/>
		private void EncryptedShuffleWithCerts(bool useClientCerts)
		{
			try
			{
				Configuration conf = new Configuration();
				string keystoresDir = new FilePath(Basedir).GetAbsolutePath();
				string sslConfsDir = KeyStoreTestUtil.GetClasspathDir(typeof(TestEncryptedShuffle
					));
				KeyStoreTestUtil.SetupSSLConfig(keystoresDir, sslConfsDir, conf, useClientCerts);
				conf.SetBoolean(MRConfig.ShuffleSslEnabledKey, true);
				StartCluster(conf);
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
			finally
			{
				StopCluster();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void EncryptedShuffleWithClientCerts()
		{
			EncryptedShuffleWithCerts(true);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void EncryptedShuffleWithoutClientCerts()
		{
			EncryptedShuffleWithCerts(false);
		}
	}
}
