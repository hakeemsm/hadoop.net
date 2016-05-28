using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Lib.Lang;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Server
{
	public class TestServer : HTestCase
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ConstructorsGetters()
		{
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", GetAbsolutePath("/a"), GetAbsolutePath("/b"), GetAbsolutePath("/c"), 
				GetAbsolutePath("/d"), new Configuration(false));
			NUnit.Framework.Assert.AreEqual(server.GetHomeDir(), GetAbsolutePath("/a"));
			NUnit.Framework.Assert.AreEqual(server.GetConfigDir(), GetAbsolutePath("/b"));
			NUnit.Framework.Assert.AreEqual(server.GetLogDir(), GetAbsolutePath("/c"));
			NUnit.Framework.Assert.AreEqual(server.GetTempDir(), GetAbsolutePath("/d"));
			NUnit.Framework.Assert.AreEqual(server.GetName(), "server");
			NUnit.Framework.Assert.AreEqual(server.GetPrefix(), "server");
			NUnit.Framework.Assert.AreEqual(server.GetPrefixedName("name"), "server.name");
			NUnit.Framework.Assert.IsNotNull(server.GetConfig());
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", GetAbsolutePath("/a"), 
				GetAbsolutePath("/b"), GetAbsolutePath("/c"), GetAbsolutePath("/d"));
			NUnit.Framework.Assert.AreEqual(server.GetHomeDir(), GetAbsolutePath("/a"));
			NUnit.Framework.Assert.AreEqual(server.GetConfigDir(), GetAbsolutePath("/b"));
			NUnit.Framework.Assert.AreEqual(server.GetLogDir(), GetAbsolutePath("/c"));
			NUnit.Framework.Assert.AreEqual(server.GetTempDir(), GetAbsolutePath("/d"));
			NUnit.Framework.Assert.AreEqual(server.GetName(), "server");
			NUnit.Framework.Assert.AreEqual(server.GetPrefix(), "server");
			NUnit.Framework.Assert.AreEqual(server.GetPrefixedName("name"), "server.name");
			NUnit.Framework.Assert.IsNull(server.GetConfig());
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", TestDirHelper.GetTestDir
				().GetAbsolutePath(), new Configuration(false));
			NUnit.Framework.Assert.AreEqual(server.GetHomeDir(), TestDirHelper.GetTestDir().GetAbsolutePath
				());
			NUnit.Framework.Assert.AreEqual(server.GetConfigDir(), TestDirHelper.GetTestDir()
				 + "/conf");
			NUnit.Framework.Assert.AreEqual(server.GetLogDir(), TestDirHelper.GetTestDir() + 
				"/log");
			NUnit.Framework.Assert.AreEqual(server.GetTempDir(), TestDirHelper.GetTestDir() +
				 "/temp");
			NUnit.Framework.Assert.AreEqual(server.GetName(), "server");
			NUnit.Framework.Assert.AreEqual(server.GetPrefix(), "server");
			NUnit.Framework.Assert.AreEqual(server.GetPrefixedName("name"), "server.name");
			NUnit.Framework.Assert.IsNotNull(server.GetConfig());
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", TestDirHelper.GetTestDir
				().GetAbsolutePath());
			NUnit.Framework.Assert.AreEqual(server.GetHomeDir(), TestDirHelper.GetTestDir().GetAbsolutePath
				());
			NUnit.Framework.Assert.AreEqual(server.GetConfigDir(), TestDirHelper.GetTestDir()
				 + "/conf");
			NUnit.Framework.Assert.AreEqual(server.GetLogDir(), TestDirHelper.GetTestDir() + 
				"/log");
			NUnit.Framework.Assert.AreEqual(server.GetTempDir(), TestDirHelper.GetTestDir() +
				 "/temp");
			NUnit.Framework.Assert.AreEqual(server.GetName(), "server");
			NUnit.Framework.Assert.AreEqual(server.GetPrefix(), "server");
			NUnit.Framework.Assert.AreEqual(server.GetPrefixedName("name"), "server.name");
			NUnit.Framework.Assert.IsNull(server.GetConfig());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InitNoHomeDir()
		{
			FilePath homeDir = new FilePath(TestDirHelper.GetTestDir(), "home");
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir.GetAbsolutePath(), conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InitHomeDirNotDir()
		{
			FilePath homeDir = new FilePath(TestDirHelper.GetTestDir(), "home");
			new FileOutputStream(homeDir).Close();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir.GetAbsolutePath(), conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InitNoConfigDir()
		{
			FilePath homeDir = new FilePath(TestDirHelper.GetTestDir(), "home");
			NUnit.Framework.Assert.IsTrue(homeDir.Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "log").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "temp").Mkdir());
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir.GetAbsolutePath(), conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InitConfigDirNotDir()
		{
			FilePath homeDir = new FilePath(TestDirHelper.GetTestDir(), "home");
			NUnit.Framework.Assert.IsTrue(homeDir.Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "log").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "temp").Mkdir());
			FilePath configDir = new FilePath(homeDir, "conf");
			new FileOutputStream(configDir).Close();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir.GetAbsolutePath(), conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InitNoLogDir()
		{
			FilePath homeDir = new FilePath(TestDirHelper.GetTestDir(), "home");
			NUnit.Framework.Assert.IsTrue(homeDir.Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "conf").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "temp").Mkdir());
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir.GetAbsolutePath(), conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InitLogDirNotDir()
		{
			FilePath homeDir = new FilePath(TestDirHelper.GetTestDir(), "home");
			NUnit.Framework.Assert.IsTrue(homeDir.Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "conf").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "temp").Mkdir());
			FilePath logDir = new FilePath(homeDir, "log");
			new FileOutputStream(logDir).Close();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir.GetAbsolutePath(), conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InitNoTempDir()
		{
			FilePath homeDir = new FilePath(TestDirHelper.GetTestDir(), "home");
			NUnit.Framework.Assert.IsTrue(homeDir.Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "conf").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "log").Mkdir());
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir.GetAbsolutePath(), conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InitTempDirNotDir()
		{
			FilePath homeDir = new FilePath(TestDirHelper.GetTestDir(), "home");
			NUnit.Framework.Assert.IsTrue(homeDir.Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "conf").Mkdir());
			NUnit.Framework.Assert.IsTrue(new FilePath(homeDir, "log").Mkdir());
			FilePath tempDir = new FilePath(homeDir, "temp");
			new FileOutputStream(tempDir).Close();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir.GetAbsolutePath(), conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void SiteFileNotAFile()
		{
			string homeDir = TestDirHelper.GetTestDir().GetAbsolutePath();
			FilePath siteFile = new FilePath(homeDir, "server-site.xml");
			NUnit.Framework.Assert.IsTrue(siteFile.Mkdir());
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", homeDir, homeDir, homeDir, homeDir);
			server.Init();
		}

		private Org.Apache.Hadoop.Lib.Server.Server CreateServer(Configuration conf)
		{
			return new Org.Apache.Hadoop.Lib.Server.Server("server", TestDirHelper.GetTestDir
				().GetAbsolutePath(), TestDirHelper.GetTestDir().GetAbsolutePath(), TestDirHelper
				.GetTestDir().GetAbsolutePath(), TestDirHelper.GetTestDir().GetAbsolutePath(), conf
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void Log4jFile()
		{
			InputStream @is = Org.Apache.Hadoop.Lib.Server.Server.GetResource("default-log4j.properties"
				);
			OutputStream os = new FileOutputStream(new FilePath(TestDirHelper.GetTestDir(), "server-log4j.properties"
				));
			IOUtils.CopyBytes(@is, os, 1024, true);
			Configuration conf = new Configuration(false);
			Org.Apache.Hadoop.Lib.Server.Server server = CreateServer(conf);
			server.Init();
		}

		public class LifeCycleService : BaseService
		{
			public LifeCycleService()
				: base("lifecycle")
			{
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			protected internal override void Init()
			{
				NUnit.Framework.Assert.AreEqual(GetServer().GetStatus(), Server.Status.Booting);
			}

			public override void Destroy()
			{
				NUnit.Framework.Assert.AreEqual(GetServer().GetStatus(), Server.Status.ShuttingDown
					);
				base.Destroy();
			}

			public override Type GetInterface()
			{
				return typeof(TestServer.LifeCycleService);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void LifeCycle()
		{
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.LifeCycleService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = CreateServer(conf);
			NUnit.Framework.Assert.AreEqual(server.GetStatus(), Server.Status.Undef);
			server.Init();
			NUnit.Framework.Assert.IsNotNull(server.Get<TestServer.LifeCycleService>());
			NUnit.Framework.Assert.AreEqual(server.GetStatus(), Server.Status.Normal);
			server.Destroy();
			NUnit.Framework.Assert.AreEqual(server.GetStatus(), Server.Status.Shutdown);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void StartWithStatusNotNormal()
		{
			Configuration conf = new Configuration(false);
			conf.Set("server.startup.status", "ADMIN");
			Org.Apache.Hadoop.Lib.Server.Server server = CreateServer(conf);
			server.Init();
			NUnit.Framework.Assert.AreEqual(server.GetStatus(), Server.Status.Admin);
			server.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[TestDir]
		public virtual void NonSeteableStatus()
		{
			Configuration conf = new Configuration(false);
			Org.Apache.Hadoop.Lib.Server.Server server = CreateServer(conf);
			server.Init();
			server.SetStatus(Server.Status.Shutdown);
		}

		public class TestService : Service
		{
			internal static IList<string> Lifecycle = new AList<string>();

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void Init(Org.Apache.Hadoop.Lib.Server.Server server)
			{
				Lifecycle.AddItem("init");
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void PostInit()
			{
				Lifecycle.AddItem("postInit");
			}

			public virtual void Destroy()
			{
				Lifecycle.AddItem("destroy");
			}

			public virtual Type[] GetServiceDependencies()
			{
				return new Type[0];
			}

			public virtual Type GetInterface()
			{
				return typeof(TestServer.TestService);
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void ServerStatusChange(Server.Status oldStatus, Server.Status newStatus
				)
			{
				Lifecycle.AddItem("serverStatusChange");
			}
		}

		public class TestServiceExceptionOnStatusChange : TestServer.TestService
		{
			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public override void ServerStatusChange(Server.Status oldStatus, Server.Status newStatus
				)
			{
				throw new RuntimeException();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ChangeStatus()
		{
			TestServer.TestService.Lifecycle.Clear();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = CreateServer(conf);
			server.Init();
			server.SetStatus(Server.Status.Admin);
			NUnit.Framework.Assert.IsTrue(TestServer.TestService.Lifecycle.Contains("serverStatusChange"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ChangeStatusServiceException()
		{
			TestServer.TestService.Lifecycle.Clear();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestServiceExceptionOnStatusChange)
				.FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = CreateServer(conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void SetSameStatus()
		{
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = CreateServer(conf);
			server.Init();
			TestServer.TestService.Lifecycle.Clear();
			server.SetStatus(server.GetStatus());
			NUnit.Framework.Assert.IsFalse(TestServer.TestService.Lifecycle.Contains("serverStatusChange"
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ServiceLifeCycle()
		{
			TestServer.TestService.Lifecycle.Clear();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.TestService).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = CreateServer(conf);
			server.Init();
			NUnit.Framework.Assert.IsNotNull(server.Get<TestServer.TestService>());
			server.Destroy();
			NUnit.Framework.Assert.AreEqual(TestServer.TestService.Lifecycle, Arrays.AsList("init"
				, "postInit", "serverStatusChange", "destroy"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void LoadingDefaultConfig()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("testserver", dir, dir, dir, dir);
			server.Init();
			NUnit.Framework.Assert.AreEqual(server.GetConfig().Get("testserver.a"), "default"
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void LoadingSiteConfig()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			FilePath configFile = new FilePath(dir, "testserver-site.xml");
			TextWriter w = new FileWriter(configFile);
			w.Write("<configuration><property><name>testserver.a</name><value>site</value></property></configuration>"
				);
			w.Close();
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("testserver", dir, dir, dir, dir);
			server.Init();
			NUnit.Framework.Assert.AreEqual(server.GetConfig().Get("testserver.a"), "site");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void LoadingSysPropConfig()
		{
			try
			{
				Runtime.SetProperty("testserver.a", "sysprop");
				string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
				FilePath configFile = new FilePath(dir, "testserver-site.xml");
				TextWriter w = new FileWriter(configFile);
				w.Write("<configuration><property><name>testserver.a</name><value>site</value></property></configuration>"
					);
				w.Close();
				Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
					("testserver", dir, dir, dir, dir);
				server.Init();
				NUnit.Framework.Assert.AreEqual(server.GetConfig().Get("testserver.a"), "sysprop"
					);
			}
			finally
			{
				Runtime.GetProperties().Remove("testserver.a");
			}
		}

		/// <exception cref="System.Exception"/>
		[TestDir]
		public virtual void IllegalState1()
		{
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", TestDirHelper.GetTestDir().GetAbsolutePath(), new Configuration(false
				));
			server.Destroy();
		}

		/// <exception cref="System.Exception"/>
		[TestDir]
		public virtual void IllegalState2()
		{
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", TestDirHelper.GetTestDir().GetAbsolutePath(), new Configuration(false
				));
			server.Get<object>();
		}

		/// <exception cref="System.Exception"/>
		[TestDir]
		public virtual void IllegalState3()
		{
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", TestDirHelper.GetTestDir().GetAbsolutePath(), new Configuration(false
				));
			server.SetService(null);
		}

		/// <exception cref="System.Exception"/>
		[TestDir]
		public virtual void IllegalState4()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, new Configuration(false));
			server.Init();
			server.Init();
		}

		private static IList<string> Order = new AList<string>();

		public abstract class MyService : Service, XException.ERROR
		{
			private string id;

			private Type serviceInterface;

			private Type[] dependencies;

			private bool failOnInit;

			private bool failOnDestroy;

			protected internal MyService(string id, Type serviceInterface, Type[] dependencies
				, bool failOnInit, bool failOnDestroy)
			{
				this.id = id;
				this.serviceInterface = serviceInterface;
				this.dependencies = dependencies;
				this.failOnInit = failOnInit;
				this.failOnDestroy = failOnDestroy;
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void Init(Org.Apache.Hadoop.Lib.Server.Server server)
			{
				Order.AddItem(id + ".init");
				if (failOnInit)
				{
					throw new ServiceException(this);
				}
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void PostInit()
			{
				Order.AddItem(id + ".postInit");
			}

			public virtual string GetTemplate()
			{
				return string.Empty;
			}

			public virtual void Destroy()
			{
				Order.AddItem(id + ".destroy");
				if (failOnDestroy)
				{
					throw new RuntimeException();
				}
			}

			public virtual Type[] GetServiceDependencies()
			{
				return dependencies;
			}

			public virtual Type GetInterface()
			{
				return serviceInterface;
			}

			/// <exception cref="Org.Apache.Hadoop.Lib.Server.ServiceException"/>
			public virtual void ServerStatusChange(Server.Status oldStatus, Server.Status newStatus
				)
			{
			}
		}

		public class MyService1 : TestServer.MyService
		{
			public MyService1()
				: base("s1", typeof(TestServer.MyService1), null, false, false)
			{
			}

			protected internal MyService1(string id, Type serviceInterface, Type[] dependencies
				, bool failOnInit, bool failOnDestroy)
				: base(id, serviceInterface, dependencies, failOnInit, failOnDestroy)
			{
			}
		}

		public class MyService2 : TestServer.MyService
		{
			public MyService2()
				: base("s2", typeof(TestServer.MyService2), null, true, false)
			{
			}
		}

		public class MyService3 : TestServer.MyService
		{
			public MyService3()
				: base("s3", typeof(TestServer.MyService3), null, false, false)
			{
			}
		}

		public class MyService1a : TestServer.MyService1
		{
			public MyService1a()
				: base("s1a", typeof(TestServer.MyService1), null, false, false)
			{
			}
		}

		public class MyService4 : TestServer.MyService1
		{
			public MyService4()
				: base("s4a", typeof(string), null, false, false)
			{
			}
		}

		public class MyService5 : TestServer.MyService
		{
			public MyService5()
				: base("s5", typeof(TestServer.MyService5), null, false, true)
			{
			}

			protected internal MyService5(string id, Type serviceInterface, Type[] dependencies
				, bool failOnInit, bool failOnDestroy)
				: base(id, serviceInterface, dependencies, failOnInit, failOnDestroy)
			{
			}
		}

		public class MyService5a : TestServer.MyService5
		{
			public MyService5a()
				: base("s5a", typeof(TestServer.MyService5), null, false, false)
			{
			}
		}

		public class MyService6 : TestServer.MyService
		{
			public MyService6()
				: base("s6", typeof(TestServer.MyService6), new Type[] { typeof(TestServer.MyService1
					) }, false, false)
			{
			}
		}

		public class MyService7 : TestServer.MyService
		{
			public MyService7(string foo)
				: base("s6", typeof(TestServer.MyService7), new Type[] { typeof(TestServer.MyService1
					) }, false, false)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void InvalidSservice()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", "foo");
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ServiceWithNoDefaultConstructor()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.MyService7).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ServiceNotImplementingServiceInterface()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration conf = new Configuration(false);
			conf.Set("server.services", typeof(TestServer.MyService4).FullName);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void ServiceWithMissingDependency()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration conf = new Configuration(false);
			string services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService3
				).FullName, typeof(TestServer.MyService6).FullName));
			conf.Set("server.services", services);
			Org.Apache.Hadoop.Lib.Server.Server server = new Org.Apache.Hadoop.Lib.Server.Server
				("server", dir, dir, dir, dir, conf);
			server.Init();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		[TestDir]
		public virtual void Services()
		{
			string dir = TestDirHelper.GetTestDir().GetAbsolutePath();
			Configuration conf;
			Org.Apache.Hadoop.Lib.Server.Server server;
			// no services
			Order.Clear();
			conf = new Configuration(false);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			NUnit.Framework.Assert.AreEqual(Order.Count, 0);
			// 2 services init/destroy
			Order.Clear();
			string services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1
				).FullName, typeof(TestServer.MyService3).FullName));
			conf = new Configuration(false);
			conf.Set("server.services", services);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			NUnit.Framework.Assert.AreEqual(server.Get<TestServer.MyService1>().GetInterface(
				), typeof(TestServer.MyService1));
			NUnit.Framework.Assert.AreEqual(server.Get<TestServer.MyService3>().GetInterface(
				), typeof(TestServer.MyService3));
			NUnit.Framework.Assert.AreEqual(Order.Count, 4);
			NUnit.Framework.Assert.AreEqual(Order[0], "s1.init");
			NUnit.Framework.Assert.AreEqual(Order[1], "s3.init");
			NUnit.Framework.Assert.AreEqual(Order[2], "s1.postInit");
			NUnit.Framework.Assert.AreEqual(Order[3], "s3.postInit");
			server.Destroy();
			NUnit.Framework.Assert.AreEqual(Order.Count, 6);
			NUnit.Framework.Assert.AreEqual(Order[4], "s3.destroy");
			NUnit.Framework.Assert.AreEqual(Order[5], "s1.destroy");
			// 3 services, 2nd one fails on init
			Order.Clear();
			services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1).FullName
				, typeof(TestServer.MyService2).FullName, typeof(TestServer.MyService3).FullName
				));
			conf = new Configuration(false);
			conf.Set("server.services", services);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			try
			{
				server.Init();
				NUnit.Framework.Assert.Fail();
			}
			catch (ServerException ex)
			{
				NUnit.Framework.Assert.AreEqual(typeof(TestServer.MyService2), ex.GetError().GetType
					());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			NUnit.Framework.Assert.AreEqual(Order.Count, 3);
			NUnit.Framework.Assert.AreEqual(Order[0], "s1.init");
			NUnit.Framework.Assert.AreEqual(Order[1], "s2.init");
			NUnit.Framework.Assert.AreEqual(Order[2], "s1.destroy");
			// 2 services one fails on destroy
			Order.Clear();
			services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1).FullName
				, typeof(TestServer.MyService5).FullName));
			conf = new Configuration(false);
			conf.Set("server.services", services);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			NUnit.Framework.Assert.AreEqual(Order.Count, 4);
			NUnit.Framework.Assert.AreEqual(Order[0], "s1.init");
			NUnit.Framework.Assert.AreEqual(Order[1], "s5.init");
			NUnit.Framework.Assert.AreEqual(Order[2], "s1.postInit");
			NUnit.Framework.Assert.AreEqual(Order[3], "s5.postInit");
			server.Destroy();
			NUnit.Framework.Assert.AreEqual(Order.Count, 6);
			NUnit.Framework.Assert.AreEqual(Order[4], "s5.destroy");
			NUnit.Framework.Assert.AreEqual(Order[5], "s1.destroy");
			// service override via ext
			Order.Clear();
			services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1).FullName
				, typeof(TestServer.MyService3).FullName));
			string servicesExt = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1a
				).FullName));
			conf = new Configuration(false);
			conf.Set("server.services", services);
			conf.Set("server.services.ext", servicesExt);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			NUnit.Framework.Assert.AreEqual(server.Get<TestServer.MyService1>().GetType(), typeof(
				TestServer.MyService1a));
			NUnit.Framework.Assert.AreEqual(Order.Count, 4);
			NUnit.Framework.Assert.AreEqual(Order[0], "s1a.init");
			NUnit.Framework.Assert.AreEqual(Order[1], "s3.init");
			NUnit.Framework.Assert.AreEqual(Order[2], "s1a.postInit");
			NUnit.Framework.Assert.AreEqual(Order[3], "s3.postInit");
			server.Destroy();
			NUnit.Framework.Assert.AreEqual(Order.Count, 6);
			NUnit.Framework.Assert.AreEqual(Order[4], "s3.destroy");
			NUnit.Framework.Assert.AreEqual(Order[5], "s1a.destroy");
			// service override via setService
			Order.Clear();
			services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1).FullName
				, typeof(TestServer.MyService3).FullName));
			conf = new Configuration(false);
			conf.Set("server.services", services);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			server.SetService(typeof(TestServer.MyService1a));
			NUnit.Framework.Assert.AreEqual(Order.Count, 6);
			NUnit.Framework.Assert.AreEqual(Order[4], "s1.destroy");
			NUnit.Framework.Assert.AreEqual(Order[5], "s1a.init");
			NUnit.Framework.Assert.AreEqual(server.Get<TestServer.MyService1>().GetType(), typeof(
				TestServer.MyService1a));
			server.Destroy();
			NUnit.Framework.Assert.AreEqual(Order.Count, 8);
			NUnit.Framework.Assert.AreEqual(Order[6], "s3.destroy");
			NUnit.Framework.Assert.AreEqual(Order[7], "s1a.destroy");
			// service add via setService
			Order.Clear();
			services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1).FullName
				, typeof(TestServer.MyService3).FullName));
			conf = new Configuration(false);
			conf.Set("server.services", services);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			server.SetService(typeof(TestServer.MyService5));
			NUnit.Framework.Assert.AreEqual(Order.Count, 5);
			NUnit.Framework.Assert.AreEqual(Order[4], "s5.init");
			NUnit.Framework.Assert.AreEqual(server.Get<TestServer.MyService5>().GetType(), typeof(
				TestServer.MyService5));
			server.Destroy();
			NUnit.Framework.Assert.AreEqual(Order.Count, 8);
			NUnit.Framework.Assert.AreEqual(Order[5], "s5.destroy");
			NUnit.Framework.Assert.AreEqual(Order[6], "s3.destroy");
			NUnit.Framework.Assert.AreEqual(Order[7], "s1.destroy");
			// service add via setService exception
			Order.Clear();
			services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1).FullName
				, typeof(TestServer.MyService3).FullName));
			conf = new Configuration(false);
			conf.Set("server.services", services);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			try
			{
				server.SetService(typeof(TestServer.MyService7));
				NUnit.Framework.Assert.Fail();
			}
			catch (ServerException ex)
			{
				NUnit.Framework.Assert.AreEqual(ServerException.ERROR.S09, ex.GetError());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail();
			}
			NUnit.Framework.Assert.AreEqual(Order.Count, 6);
			NUnit.Framework.Assert.AreEqual(Order[4], "s3.destroy");
			NUnit.Framework.Assert.AreEqual(Order[5], "s1.destroy");
			// service with dependency
			Order.Clear();
			services = StringUtils.Join(",", Arrays.AsList(typeof(TestServer.MyService1).FullName
				, typeof(TestServer.MyService6).FullName));
			conf = new Configuration(false);
			conf.Set("server.services", services);
			server = new Org.Apache.Hadoop.Lib.Server.Server("server", dir, dir, dir, dir, conf
				);
			server.Init();
			NUnit.Framework.Assert.AreEqual(server.Get<TestServer.MyService1>().GetInterface(
				), typeof(TestServer.MyService1));
			NUnit.Framework.Assert.AreEqual(server.Get<TestServer.MyService6>().GetInterface(
				), typeof(TestServer.MyService6));
			server.Destroy();
		}

		/// <summary>
		/// Creates an absolute path by appending the given relative path to the test
		/// root.
		/// </summary>
		/// <param name="relativePath">String relative path</param>
		/// <returns>String absolute path formed by appending relative path to test root</returns>
		private static string GetAbsolutePath(string relativePath)
		{
			return new FilePath(TestDirHelper.GetTestDir(), relativePath).GetAbsolutePath();
		}
	}
}
