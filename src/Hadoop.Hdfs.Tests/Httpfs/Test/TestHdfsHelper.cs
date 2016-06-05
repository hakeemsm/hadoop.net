using System;
using NUnit.Framework.Runners.Model;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	public class TestHdfsHelper : TestDirHelper
	{
		[NUnit.Framework.Test]
		public override void Dummy()
		{
		}

		public const string HadoopMiniHdfs = "test.hadoop.hdfs";

		private static ThreadLocal<Configuration> HdfsConfTl = new InheritableThreadLocal
			<Configuration>();

		private static ThreadLocal<Path> HdfsTestDirTl = new InheritableThreadLocal<Path>
			();

		public override Statement Apply(Statement statement, FrameworkMethod frameworkMethod
			, object o)
		{
			TestHdfs testHdfsAnnotation = frameworkMethod.GetAnnotation<TestHdfs>();
			if (testHdfsAnnotation != null)
			{
				statement = new TestHdfsHelper.HdfsStatement(statement, frameworkMethod.GetName()
					);
			}
			return base.Apply(statement, frameworkMethod, o);
		}

		private class HdfsStatement : Statement
		{
			private Statement statement;

			private string testName;

			public HdfsStatement(Statement statement, string testName)
			{
				this.statement = statement;
				this.testName = testName;
			}

			/// <exception cref="System.Exception"/>
			public override void Evaluate()
			{
				MiniDFSCluster miniHdfs = null;
				Configuration conf = HadoopUsersConfTestHelper.GetBaseConf();
				if (System.Boolean.Parse(Runtime.GetProperty(HadoopMiniHdfs, "true")))
				{
					miniHdfs = StartMiniHdfs(conf);
					conf = miniHdfs.GetConfiguration(0);
				}
				try
				{
					HdfsConfTl.Set(conf);
					HdfsTestDirTl.Set(ResetHdfsTestDir(conf));
					statement.Evaluate();
				}
				finally
				{
					HdfsConfTl.Remove();
					HdfsTestDirTl.Remove();
				}
			}

			private static AtomicInteger counter = new AtomicInteger();

			private Path ResetHdfsTestDir(Configuration conf)
			{
				Path testDir = new Path("/tmp/" + testName + "-" + counter.GetAndIncrement());
				try
				{
					// currentUser
					FileSystem fs = FileSystem.Get(conf);
					fs.Delete(testDir, true);
					fs.Mkdirs(testDir);
				}
				catch (Exception ex)
				{
					throw new RuntimeException(ex);
				}
				return testDir;
			}
		}

		/// <summary>
		/// Returns the HDFS test directory for the current test, only available when the
		/// test method has been annotated with
		/// <see cref="TestHdfs"/>
		/// .
		/// </summary>
		/// <returns>
		/// the HDFS test directory for the current test. It is an full/absolute
		/// <code>Path</code>.
		/// </returns>
		public static Path GetHdfsTestDir()
		{
			Path testDir = HdfsTestDirTl.Get();
			if (testDir == null)
			{
				throw new InvalidOperationException("This test does not use @TestHdfs");
			}
			return testDir;
		}

		/// <summary>
		/// Returns a FileSystemAccess <code>JobConf</code> preconfigured with the FileSystemAccess cluster
		/// settings for testing.
		/// </summary>
		/// <remarks>
		/// Returns a FileSystemAccess <code>JobConf</code> preconfigured with the FileSystemAccess cluster
		/// settings for testing. This configuration is only available when the test
		/// method has been annotated with
		/// <see cref="TestHdfs"/>
		/// . Refer to
		/// <see cref="HTestCase"/>
		/// header for details)
		/// </remarks>
		/// <returns>
		/// the FileSystemAccess <code>JobConf</code> preconfigured with the FileSystemAccess cluster
		/// settings for testing
		/// </returns>
		public static Configuration GetHdfsConf()
		{
			Configuration conf = HdfsConfTl.Get();
			if (conf == null)
			{
				throw new InvalidOperationException("This test does not use @TestHdfs");
			}
			return new Configuration(conf);
		}

		private static MiniDFSCluster MiniDfs = null;

		/// <exception cref="System.Exception"/>
		private static MiniDFSCluster StartMiniHdfs(Configuration conf)
		{
			lock (typeof(TestHdfsHelper))
			{
				if (MiniDfs == null)
				{
					if (Runtime.GetProperty("hadoop.log.dir") == null)
					{
						Runtime.SetProperty("hadoop.log.dir", new FilePath(TestDirRoot, "hadoop-log").GetAbsolutePath
							());
					}
					if (Runtime.GetProperty("test.build.data") == null)
					{
						Runtime.SetProperty("test.build.data", new FilePath(TestDirRoot, "hadoop-data").GetAbsolutePath
							());
					}
					conf = new Configuration(conf);
					HadoopUsersConfTestHelper.AddUserConf(conf);
					conf.Set("fs.hdfs.impl.disable.cache", "true");
					conf.Set("dfs.block.access.token.enable", "false");
					conf.Set("dfs.permissions", "true");
					conf.Set("hadoop.security.authentication", "simple");
					conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
					conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
					MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
					builder.NumDataNodes(2);
					MiniDFSCluster miniHdfs = builder.Build();
					FileSystem fileSystem = miniHdfs.GetFileSystem();
					fileSystem.Mkdirs(new Path("/tmp"));
					fileSystem.Mkdirs(new Path("/user"));
					fileSystem.SetPermission(new Path("/tmp"), FsPermission.ValueOf("-rwxrwxrwx"));
					fileSystem.SetPermission(new Path("/user"), FsPermission.ValueOf("-rwxrwxrwx"));
					MiniDfs = miniHdfs;
				}
				return MiniDfs;
			}
		}
	}
}
