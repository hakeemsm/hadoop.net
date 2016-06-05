using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Lib.Service;
using Org.Apache.Hadoop.Lib.Service.Instrumentation;
using Org.Apache.Hadoop.Lib.Service.Scheduler;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service.Hadoop
{
	public class TestFileSystemAccessService : HFSTestCase
	{
		/// <exception cref="System.Exception"/>
		private void CreateHadoopConf(Configuration hadoopConf)
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			FilePath hdfsSite = new FilePath(dir, "hdfs-site.xml");
			OutputStream os = new FileOutputStream(hdfsSite);
			hadoopConf.WriteXml(os);
			os.Close();
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void CreateHadoopConf()
		{
			Configuration hadoopConf = new Configuration(false);
			hadoopConf.Set("foo", "FOO");
			CreateHadoopConf(hadoopConf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void SimpleSecurity()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			NUnit.Framework.Assert.IsNotNull(server.Get<FileSystemAccess>());
			server.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void NoKerberosKeytabProperty()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.authentication.type", "kerberos");
			conf.Set("server.hadoop.authentication.kerberos.keytab", " ");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void NoKerberosPrincipalProperty()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.authentication.type", "kerberos");
			conf.Set("server.hadoop.authentication.kerberos.keytab", "/tmp/foo");
			conf.Set("server.hadoop.authentication.kerberos.principal", " ");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void KerberosInitializationFailure()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.authentication.type", "kerberos");
			conf.Set("server.hadoop.authentication.kerberos.keytab", "/tmp/foo");
			conf.Set("server.hadoop.authentication.kerberos.principal", "foo@FOO");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InvalidSecurity()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.authentication.type", "foo");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ServiceHadoopConf()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			FileSystemAccessService fsAccess = (FileSystemAccessService)server.Get<FileSystemAccess
				>();
			NUnit.Framework.Assert.AreEqual(fsAccess.serviceHadoopConf.Get("foo"), "FOO");
			server.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ServiceHadoopConfCustomDir()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string hadoopConfDir = new FilePath(dir, "confx").GetAbsolutePath();
			new FilePath(hadoopConfDir).Mkdirs();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.config.dir", hadoopConfDir);
			FilePath hdfsSite = new FilePath(hadoopConfDir, "hdfs-site.xml");
			OutputStream os = new FileOutputStream(hdfsSite);
			Configuration hadoopConf = new Configuration(false);
			hadoopConf.Set("foo", "BAR");
			hadoopConf.WriteXml(os);
			os.Close();
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			FileSystemAccessService fsAccess = (FileSystemAccessService)server.Get<FileSystemAccess
				>();
			NUnit.Framework.Assert.AreEqual(fsAccess.serviceHadoopConf.Get("foo"), "BAR");
			server.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InWhitelists()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			FileSystemAccessService fsAccess = (FileSystemAccessService)server.Get<FileSystemAccess
				>();
			fsAccess.ValidateNamenode("NN");
			server.Destroy();
			conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.name.node.whitelist", "*");
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			fsAccess = (FileSystemAccessService)server.Get<FileSystemAccess>();
			fsAccess.ValidateNamenode("NN");
			server.Destroy();
			conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.name.node.whitelist", "NN");
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			fsAccess = (FileSystemAccessService)server.Get<FileSystemAccess>();
			fsAccess.ValidateNamenode("NN");
			server.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void NameNodeNotinWhitelists()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.name.node.whitelist", "NN");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			FileSystemAccessService fsAccess = (FileSystemAccessService)server.Get<FileSystemAccess
				>();
			fsAccess.ValidateNamenode("NNx");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestHdfs]
		public virtual void CreateFileSystem()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration hadoopConf = new Configuration(false);
			hadoopConf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, TestHdfsHelper.GetHdfsConf
				().Get(CommonConfigurationKeysPublic.FsDefaultNameKey));
			CreateHadoopConf(hadoopConf);
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.filesystem.cache.purge.timeout", "0");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			FileSystemAccess hadoop = server.Get<FileSystemAccess>();
			FileSystem fs = hadoop.CreateFileSystem("u", hadoop.GetFileSystemConfiguration());
			NUnit.Framework.Assert.IsNotNull(fs);
			fs.Mkdirs(new Path("/tmp/foo"));
			hadoop.ReleaseFileSystem(fs);
			try
			{
				fs.Mkdirs(new Path("/tmp/foo"));
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			server.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestHdfs]
		public virtual void FileSystemExecutor()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration hadoopConf = new Configuration(false);
			hadoopConf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, TestHdfsHelper.GetHdfsConf
				().Get(CommonConfigurationKeysPublic.FsDefaultNameKey));
			CreateHadoopConf(hadoopConf);
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.filesystem.cache.purge.timeout", "0");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			FileSystemAccess hadoop = server.Get<FileSystemAccess>();
			FileSystem[] fsa = new FileSystem[1];
			hadoop.Execute("u", hadoop.GetFileSystemConfiguration(), new _FileSystemExecutor_306
				(fsa));
			try
			{
				fsa[0].Mkdirs(new Path("/tmp/foo"));
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			server.Destroy();
		}

		private sealed class _FileSystemExecutor_306 : FileSystemAccess.FileSystemExecutor
			<Void>
		{
			public _FileSystemExecutor_306(FileSystem[] fsa)
			{
				this.fsa = fsa;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Execute(FileSystem fs)
			{
				fs.Mkdirs(new Path("/tmp/foo"));
				fsa[0] = fs;
				return null;
			}

			private readonly FileSystem[] fsa;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestHdfs]
		public virtual void FileSystemExecutorNoNameNode()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration hadoopConf = new Configuration(false);
			hadoopConf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, TestHdfsHelper.GetHdfsConf
				().Get(CommonConfigurationKeysPublic.FsDefaultNameKey));
			CreateHadoopConf(hadoopConf);
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			FileSystemAccess fsAccess = server.Get<FileSystemAccess>();
			Configuration hdfsConf = fsAccess.GetFileSystemConfiguration();
			hdfsConf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, string.Empty);
			fsAccess.Execute("u", hdfsConf, new _FileSystemExecutor_346());
		}

		private sealed class _FileSystemExecutor_346 : FileSystemAccess.FileSystemExecutor
			<Void>
		{
			public _FileSystemExecutor_346()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Execute(FileSystem fs)
			{
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestHdfs]
		public virtual void FileSystemExecutorException()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration hadoopConf = new Configuration(false);
			hadoopConf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, TestHdfsHelper.GetHdfsConf
				().Get(CommonConfigurationKeysPublic.FsDefaultNameKey));
			CreateHadoopConf(hadoopConf);
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.filesystem.cache.purge.timeout", "0");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
			FileSystemAccess hadoop = server.Get<FileSystemAccess>();
			FileSystem[] fsa = new FileSystem[1];
			try
			{
				hadoop.Execute("u", hadoop.GetFileSystemConfiguration(), new _FileSystemExecutor_377
					(fsa));
				NUnit.Framework.Assert.Fail();
			}
			catch (FileSystemAccessException ex)
			{
				NUnit.Framework.Assert.AreEqual(ex.GetError(), FileSystemAccessException.ERROR.H03
					);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				fsa[0].Mkdirs(new Path("/tmp/foo"));
				NUnit.Framework.Assert.Fail();
			}
			catch (IOException)
			{
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			server.Destroy();
		}

		private sealed class _FileSystemExecutor_377 : FileSystemAccess.FileSystemExecutor
			<Void>
		{
			public _FileSystemExecutor_377(FileSystem[] fsa)
			{
				this.fsa = fsa;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Execute(FileSystem fs)
			{
				fsa[0] = fs;
				throw new IOException();
			}

			private readonly FileSystem[] fsa;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		[TestHdfs]
		public virtual void FileSystemCache()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(InstrumentationService
				).FullName, typeof(SchedulerService).FullName, typeof(FileSystemAccessService).FullName
				));
			Configuration hadoopConf = new Configuration(false);
			hadoopConf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, TestHdfsHelper.GetHdfsConf
				().Get(CommonConfigurationKeysPublic.FsDefaultNameKey));
			CreateHadoopConf(hadoopConf);
			Configuration conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.hadoop.filesystem.cache.purge.frequency", "1");
			conf.Set("server.hadoop.filesystem.cache.purge.timeout", "1");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			try
			{
				server.Init();
				FileSystemAccess hadoop = server.Get<FileSystemAccess>();
				FileSystem fs1 = hadoop.CreateFileSystem("u", hadoop.GetFileSystemConfiguration()
					);
				NUnit.Framework.Assert.IsNotNull(fs1);
				fs1.Mkdirs(new Path("/tmp/foo1"));
				hadoop.ReleaseFileSystem(fs1);
				//still around because of caching
				fs1.Mkdirs(new Path("/tmp/foo2"));
				FileSystem fs2 = hadoop.CreateFileSystem("u", hadoop.GetFileSystemConfiguration()
					);
				//should be same instance because of caching
				NUnit.Framework.Assert.AreEqual(fs1, fs2);
				Sharpen.Thread.Sleep(4 * 1000);
				//still around because of lease count is 1 (fs2 is out)
				fs1.Mkdirs(new Path("/tmp/foo2"));
				Sharpen.Thread.Sleep(4 * 1000);
				//still around because of lease count is 1 (fs2 is out)
				fs2.Mkdirs(new Path("/tmp/foo"));
				hadoop.ReleaseFileSystem(fs2);
				Sharpen.Thread.Sleep(4 * 1000);
				//should not be around as lease count is 0
				try
				{
					fs2.Mkdirs(new Path("/tmp/foo"));
					NUnit.Framework.Assert.Fail();
				}
				catch (IOException)
				{
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail();
				}
			}
			finally
			{
				server.Destroy();
			}
		}
	}
}
