using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager
{
	public class TestAuxServices
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.TestAuxServices
			));

		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, Runtime.GetProperty("java.io.tmpdir")), typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.TestAuxServices
			).FullName);

		internal class LightService : AuxiliaryService, Org.Apache.Hadoop.Service.Service
		{
			private readonly char idef;

			private readonly int expected_appId;

			private int remaining_init;

			private int remaining_stop;

			private ByteBuffer meta = null;

			private AList<int> stoppedApps;

			private ContainerId containerId;

			private Resource resource;

			internal LightService(string name, char idef, int expected_appId)
				: this(name, idef, expected_appId, null)
			{
			}

			internal LightService(string name, char idef, int expected_appId, ByteBuffer meta
				)
				: base(name)
			{
				this.idef = idef;
				this.expected_appId = expected_appId;
				this.meta = meta;
				this.stoppedApps = new AList<int>();
			}

			public virtual AList<int> GetAppIdsStopped()
			{
				return (AList<int>)this.stoppedApps.Clone();
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				remaining_init = conf.GetInt(idef + ".expected.init", 0);
				remaining_stop = conf.GetInt(idef + ".expected.stop", 0);
				base.ServiceInit(conf);
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStop()
			{
				NUnit.Framework.Assert.AreEqual(0, remaining_init);
				NUnit.Framework.Assert.AreEqual(0, remaining_stop);
				base.ServiceStop();
			}

			public override void InitializeApplication(ApplicationInitializationContext context
				)
			{
				ByteBuffer data = context.GetApplicationDataForService();
				NUnit.Framework.Assert.AreEqual(idef, data.GetChar());
				NUnit.Framework.Assert.AreEqual(expected_appId, data.GetInt());
				NUnit.Framework.Assert.AreEqual(expected_appId, context.GetApplicationId().GetId(
					));
			}

			public override void StopApplication(ApplicationTerminationContext context)
			{
				stoppedApps.AddItem(context.GetApplicationId().GetId());
			}

			public override ByteBuffer GetMetaData()
			{
				return meta;
			}

			public override void InitializeContainer(ContainerInitializationContext initContainerContext
				)
			{
				containerId = initContainerContext.GetContainerId();
				resource = initContainerContext.GetResource();
			}

			public override void StopContainer(ContainerTerminationContext stopContainerContext
				)
			{
				containerId = stopContainerContext.GetContainerId();
				resource = stopContainerContext.GetResource();
			}
		}

		internal class ServiceA : TestAuxServices.LightService
		{
			public ServiceA()
				: base("A", 'A', 65, ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("A")))
			{
			}
		}

		internal class ServiceB : TestAuxServices.LightService
		{
			public ServiceB()
				: base("B", 'B', 66, ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("B")))
			{
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAuxEventDispatch()
		{
			Configuration conf = new Configuration();
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { "Asrv", "Bsrv" });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Asrv"), typeof(TestAuxServices.ServiceA
				), typeof(Org.Apache.Hadoop.Service.Service));
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Bsrv"), typeof(TestAuxServices.ServiceB
				), typeof(Org.Apache.Hadoop.Service.Service));
			conf.SetInt("A.expected.init", 1);
			conf.SetInt("B.expected.stop", 1);
			AuxServices aux = new AuxServices();
			aux.Init(conf);
			aux.Start();
			ApplicationId appId1 = ApplicationId.NewInstance(0, 65);
			ByteBuffer buf = ByteBuffer.Allocate(6);
			buf.PutChar('A');
			buf.PutInt(65);
			buf.Flip();
			AuxServicesEvent @event = new AuxServicesEvent(AuxServicesEventType.ApplicationInit
				, "user0", appId1, "Asrv", buf);
			aux.Handle(@event);
			ApplicationId appId2 = ApplicationId.NewInstance(0, 66);
			@event = new AuxServicesEvent(AuxServicesEventType.ApplicationStop, "user0", appId2
				, "Bsrv", null);
			// verify all services got the stop event 
			aux.Handle(@event);
			ICollection<AuxiliaryService> servs = aux.GetServices();
			foreach (AuxiliaryService serv in servs)
			{
				AList<int> appIds = ((TestAuxServices.LightService)serv).GetAppIdsStopped();
				NUnit.Framework.Assert.AreEqual("app not properly stopped", 1, appIds.Count);
				NUnit.Framework.Assert.IsTrue("wrong app stopped", appIds.Contains((int)66));
			}
			foreach (AuxiliaryService serv_1 in servs)
			{
				NUnit.Framework.Assert.IsNull(((TestAuxServices.LightService)serv_1).containerId);
				NUnit.Framework.Assert.IsNull(((TestAuxServices.LightService)serv_1).resource);
			}
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId1, 1);
			ContainerTokenIdentifier cti = new ContainerTokenIdentifier(ContainerId.NewContainerId
				(attemptId, 1), string.Empty, string.Empty, Resource.NewInstance(1, 1), 0, 0, 0, 
				Priority.NewInstance(0), 0);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = new ContainerImpl(null, null, null, null, null, null, cti);
			ContainerId containerId = container.GetContainerId();
			Resource resource = container.GetResource();
			@event = new AuxServicesEvent(AuxServicesEventType.ContainerInit, container);
			aux.Handle(@event);
			foreach (AuxiliaryService serv_2 in servs)
			{
				NUnit.Framework.Assert.AreEqual(containerId, ((TestAuxServices.LightService)serv_2
					).containerId);
				NUnit.Framework.Assert.AreEqual(resource, ((TestAuxServices.LightService)serv_2).
					resource);
				((TestAuxServices.LightService)serv_2).containerId = null;
				((TestAuxServices.LightService)serv_2).resource = null;
			}
			@event = new AuxServicesEvent(AuxServicesEventType.ContainerStop, container);
			aux.Handle(@event);
			foreach (AuxiliaryService serv_3 in servs)
			{
				NUnit.Framework.Assert.AreEqual(containerId, ((TestAuxServices.LightService)serv_3
					).containerId);
				NUnit.Framework.Assert.AreEqual(resource, ((TestAuxServices.LightService)serv_3).
					resource);
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAuxServices()
		{
			Configuration conf = new Configuration();
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { "Asrv", "Bsrv" });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Asrv"), typeof(TestAuxServices.ServiceA
				), typeof(Org.Apache.Hadoop.Service.Service));
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Bsrv"), typeof(TestAuxServices.ServiceB
				), typeof(Org.Apache.Hadoop.Service.Service));
			AuxServices aux = new AuxServices();
			aux.Init(conf);
			int latch = 1;
			foreach (Org.Apache.Hadoop.Service.Service s in aux.GetServices())
			{
				NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, s.GetServiceState());
				if (s is TestAuxServices.ServiceA)
				{
					latch *= 2;
				}
				else
				{
					if (s is TestAuxServices.ServiceB)
					{
						latch *= 3;
					}
					else
					{
						NUnit.Framework.Assert.Fail("Unexpected service type " + s.GetType());
					}
				}
			}
			NUnit.Framework.Assert.AreEqual("Invalid mix of services", 6, latch);
			aux.Start();
			foreach (Org.Apache.Hadoop.Service.Service s_1 in aux.GetServices())
			{
				NUnit.Framework.Assert.AreEqual(Service.STATE.Started, s_1.GetServiceState());
			}
			aux.Stop();
			foreach (Org.Apache.Hadoop.Service.Service s_2 in aux.GetServices())
			{
				NUnit.Framework.Assert.AreEqual(Service.STATE.Stopped, s_2.GetServiceState());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAuxServicesMeta()
		{
			Configuration conf = new Configuration();
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { "Asrv", "Bsrv" });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Asrv"), typeof(TestAuxServices.ServiceA
				), typeof(Org.Apache.Hadoop.Service.Service));
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Bsrv"), typeof(TestAuxServices.ServiceB
				), typeof(Org.Apache.Hadoop.Service.Service));
			AuxServices aux = new AuxServices();
			aux.Init(conf);
			int latch = 1;
			foreach (Org.Apache.Hadoop.Service.Service s in aux.GetServices())
			{
				NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, s.GetServiceState());
				if (s is TestAuxServices.ServiceA)
				{
					latch *= 2;
				}
				else
				{
					if (s is TestAuxServices.ServiceB)
					{
						latch *= 3;
					}
					else
					{
						NUnit.Framework.Assert.Fail("Unexpected service type " + s.GetType());
					}
				}
			}
			NUnit.Framework.Assert.AreEqual("Invalid mix of services", 6, latch);
			aux.Start();
			foreach (Org.Apache.Hadoop.Service.Service s_1 in aux.GetServices())
			{
				NUnit.Framework.Assert.AreEqual(Service.STATE.Started, s_1.GetServiceState());
			}
			IDictionary<string, ByteBuffer> meta = aux.GetMetaData();
			NUnit.Framework.Assert.AreEqual(2, meta.Count);
			NUnit.Framework.Assert.AreEqual("A", Sharpen.Runtime.GetStringForBytes(((byte[])meta
				["Asrv"].Array())));
			NUnit.Framework.Assert.AreEqual("B", Sharpen.Runtime.GetStringForBytes(((byte[])meta
				["Bsrv"].Array())));
			aux.Stop();
			foreach (Org.Apache.Hadoop.Service.Service s_2 in aux.GetServices())
			{
				NUnit.Framework.Assert.AreEqual(Service.STATE.Stopped, s_2.GetServiceState());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAuxUnexpectedStop()
		{
			Configuration conf = new Configuration();
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { "Asrv", "Bsrv" });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Asrv"), typeof(TestAuxServices.ServiceA
				), typeof(Org.Apache.Hadoop.Service.Service));
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Bsrv"), typeof(TestAuxServices.ServiceB
				), typeof(Org.Apache.Hadoop.Service.Service));
			AuxServices aux = new AuxServices();
			aux.Init(conf);
			aux.Start();
			Org.Apache.Hadoop.Service.Service s = aux.GetServices().GetEnumerator().Next();
			s.Stop();
			NUnit.Framework.Assert.AreEqual("Auxiliary service stopped, but AuxService unaffected."
				, Service.STATE.Stopped, aux.GetServiceState());
			NUnit.Framework.Assert.IsTrue(aux.GetServices().IsEmpty());
		}

		[NUnit.Framework.Test]
		public virtual void TestValidAuxServiceName()
		{
			AuxServices aux = new AuxServices();
			Configuration conf = new Configuration();
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { "Asrv1", "Bsrv_2"
				 });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Asrv1"), typeof(TestAuxServices.ServiceA
				), typeof(Org.Apache.Hadoop.Service.Service));
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Bsrv_2"), typeof(
				TestAuxServices.ServiceB), typeof(Org.Apache.Hadoop.Service.Service));
			try
			{
				aux.Init(conf);
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not receive the exception.");
			}
			//Test bad auxService Name
			AuxServices aux1 = new AuxServices();
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { "1Asrv1" });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "1Asrv1"), typeof(
				TestAuxServices.ServiceA), typeof(Org.Apache.Hadoop.Service.Service));
			try
			{
				aux1.Init(conf);
				NUnit.Framework.Assert.Fail("Should receive the exception.");
			}
			catch (Exception ex)
			{
				NUnit.Framework.Assert.IsTrue(ex.Message.Contains("The ServiceName: 1Asrv1 set in "
					 + "yarn.nodemanager.aux-services is invalid.The valid service name " + "should only contain a-zA-Z0-9_ and can not start with numbers"
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAuxServiceRecoverySetup()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.NmRecoveryEnabled, true);
			conf.Set(YarnConfiguration.NmRecoveryDir, TestDir.ToString());
			conf.SetStrings(YarnConfiguration.NmAuxServices, new string[] { "Asrv", "Bsrv" });
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Asrv"), typeof(TestAuxServices.RecoverableServiceA
				), typeof(Org.Apache.Hadoop.Service.Service));
			conf.SetClass(string.Format(YarnConfiguration.NmAuxServiceFmt, "Bsrv"), typeof(TestAuxServices.RecoverableServiceB
				), typeof(Org.Apache.Hadoop.Service.Service));
			try
			{
				AuxServices aux = new AuxServices();
				aux.Init(conf);
				NUnit.Framework.Assert.AreEqual(2, aux.GetServices().Count);
				FilePath auxStorageDir = new FilePath(TestDir, AuxServices.StateStoreRootName);
				NUnit.Framework.Assert.AreEqual(2, auxStorageDir.ListFiles().Length);
				aux.Close();
			}
			finally
			{
				FileUtil.FullyDelete(TestDir);
			}
		}

		internal class RecoverableAuxService : AuxiliaryService
		{
			internal static readonly FsPermission RecoveryPathPerms = new FsPermission((short
				)0x1c0);

			internal string auxName;

			internal RecoverableAuxService(string name, string auxName)
				: base(name)
			{
				this.auxName = auxName;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceInit(Configuration conf)
			{
				base.ServiceInit(conf);
				Path storagePath = GetRecoveryPath();
				NUnit.Framework.Assert.IsNotNull("Recovery path not present when aux service inits"
					, storagePath);
				NUnit.Framework.Assert.IsTrue(storagePath.ToString().Contains(auxName));
				FileSystem fs = FileSystem.GetLocal(conf);
				NUnit.Framework.Assert.IsTrue("Recovery path does not exist", fs.Exists(storagePath
					));
				NUnit.Framework.Assert.AreEqual("Recovery path has wrong permissions", new FsPermission
					((short)0x1c0), fs.GetFileStatus(storagePath).GetPermission());
			}

			public override void InitializeApplication(ApplicationInitializationContext initAppContext
				)
			{
			}

			public override void StopApplication(ApplicationTerminationContext stopAppContext
				)
			{
			}

			public override ByteBuffer GetMetaData()
			{
				return null;
			}
		}

		internal class RecoverableServiceA : TestAuxServices.RecoverableAuxService
		{
			internal RecoverableServiceA()
				: base("RecoverableServiceA", "Asrv")
			{
			}
		}

		internal class RecoverableServiceB : TestAuxServices.RecoverableAuxService
		{
			internal RecoverableServiceB()
				: base("RecoverableServiceB", "Bsrv")
			{
			}
		}
	}
}
