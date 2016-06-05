using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Service
{
	public class TestCompositeService
	{
		private const int NumOfServices = 5;

		private const int FailedServiceSeqNumber = 2;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestCompositeService));

		/// <summary>
		/// flag to state policy of CompositeService, and hence
		/// what to look for after trying to stop a service from another state
		/// (e.g inited)
		/// </summary>
		private static readonly bool StopOnlyStartedServices = TestCompositeService.CompositeServiceImpl
			.IsPolicyToStopOnlyStartedServices();

		[SetUp]
		public virtual void Setup()
		{
			TestCompositeService.CompositeServiceImpl.ResetCounter();
		}

		[Fact]
		public virtual void TestCallSequence()
		{
			TestCompositeService.ServiceManager serviceManager = new TestCompositeService.ServiceManager
				("ServiceManager");
			// Add services
			for (int i = 0; i < NumOfServices; i++)
			{
				TestCompositeService.CompositeServiceImpl service = new TestCompositeService.CompositeServiceImpl
					(i);
				serviceManager.AddTestService(service);
			}
			TestCompositeService.CompositeServiceImpl[] services = Collections.ToArray
				(serviceManager.GetServices(), new TestCompositeService.CompositeServiceImpl[0]);
			Assert.Equal("Number of registered services ", NumOfServices, 
				services.Length);
			Configuration conf = new Configuration();
			// Initialise the composite service
			serviceManager.Init(conf);
			//verify they were all inited
			AssertInState(Service.STATE.Inited, services);
			// Verify the init() call sequence numbers for every service
			for (int i_1 = 0; i_1 < NumOfServices; i_1++)
			{
				Assert.Equal("For " + services[i_1] + " service, init() call sequence number should have been "
					, i_1, services[i_1].GetCallSequenceNumber());
			}
			// Reset the call sequence numbers
			ResetServices(services);
			serviceManager.Start();
			//verify they were all started
			AssertInState(Service.STATE.Started, services);
			// Verify the start() call sequence numbers for every service
			for (int i_2 = 0; i_2 < NumOfServices; i_2++)
			{
				Assert.Equal("For " + services[i_2] + " service, start() call sequence number should have been "
					, i_2, services[i_2].GetCallSequenceNumber());
			}
			ResetServices(services);
			serviceManager.Stop();
			//verify they were all stopped
			AssertInState(Service.STATE.Stopped, services);
			// Verify the stop() call sequence numbers for every service
			for (int i_3 = 0; i_3 < NumOfServices; i_3++)
			{
				Assert.Equal("For " + services[i_3] + " service, stop() call sequence number should have been "
					, ((NumOfServices - 1) - i_3), services[i_3].GetCallSequenceNumber());
			}
			// Try to stop again. This should be a no-op.
			serviceManager.Stop();
			// Verify that stop() call sequence numbers for every service don't change.
			for (int i_4 = 0; i_4 < NumOfServices; i_4++)
			{
				Assert.Equal("For " + services[i_4] + " service, stop() call sequence number should have been "
					, ((NumOfServices - 1) - i_4), services[i_4].GetCallSequenceNumber());
			}
		}

		private void ResetServices(TestCompositeService.CompositeServiceImpl[] services)
		{
			// Reset the call sequence numbers
			for (int i = 0; i < NumOfServices; i++)
			{
				services[i].Reset();
			}
		}

		[Fact]
		public virtual void TestServiceStartup()
		{
			TestCompositeService.ServiceManager serviceManager = new TestCompositeService.ServiceManager
				("ServiceManager");
			// Add services
			for (int i = 0; i < NumOfServices; i++)
			{
				TestCompositeService.CompositeServiceImpl service = new TestCompositeService.CompositeServiceImpl
					(i);
				if (i == FailedServiceSeqNumber)
				{
					service.SetThrowExceptionOnStart(true);
				}
				serviceManager.AddTestService(service);
			}
			TestCompositeService.CompositeServiceImpl[] services = Collections.ToArray
				(serviceManager.GetServices(), new TestCompositeService.CompositeServiceImpl[0]);
			Configuration conf = new Configuration();
			// Initialise the composite service
			serviceManager.Init(conf);
			// Start the composite service
			try
			{
				serviceManager.Start();
				NUnit.Framework.Assert.Fail("Exception should have been thrown due to startup failure of last service"
					);
			}
			catch (TestCompositeService.ServiceTestRuntimeException)
			{
				for (int i_1 = 0; i_1 < NumOfServices - 1; i_1++)
				{
					if (i_1 >= FailedServiceSeqNumber && StopOnlyStartedServices)
					{
						// Failed service state should be INITED
						Assert.Equal("Service state should have been ", Service.STATE.
							Inited, services[NumOfServices - 1].GetServiceState());
					}
					else
					{
						Assert.Equal("Service state should have been ", Service.STATE.
							Stopped, services[i_1].GetServiceState());
					}
				}
			}
		}

		[Fact]
		public virtual void TestServiceStop()
		{
			TestCompositeService.ServiceManager serviceManager = new TestCompositeService.ServiceManager
				("ServiceManager");
			// Add services
			for (int i = 0; i < NumOfServices; i++)
			{
				TestCompositeService.CompositeServiceImpl service = new TestCompositeService.CompositeServiceImpl
					(i);
				if (i == FailedServiceSeqNumber)
				{
					service.SetThrowExceptionOnStop(true);
				}
				serviceManager.AddTestService(service);
			}
			TestCompositeService.CompositeServiceImpl[] services = Collections.ToArray
				(serviceManager.GetServices(), new TestCompositeService.CompositeServiceImpl[0]);
			Configuration conf = new Configuration();
			// Initialise the composite service
			serviceManager.Init(conf);
			serviceManager.Start();
			// Stop the composite service
			try
			{
				serviceManager.Stop();
			}
			catch (TestCompositeService.ServiceTestRuntimeException)
			{
			}
			AssertInState(Service.STATE.Stopped, services);
		}

		/// <summary>Assert that all services are in the same expected state</summary>
		/// <param name="expected">expected state value</param>
		/// <param name="services">services to examine</param>
		private void AssertInState(Service.STATE expected, TestCompositeService.CompositeServiceImpl
			[] services)
		{
			AssertInState(expected, services, 0, services.Length);
		}

		/// <summary>Assert that all services are in the same expected state</summary>
		/// <param name="expected">expected state value</param>
		/// <param name="services">services to examine</param>
		/// <param name="start">start offset</param>
		/// <param name="finish">finish offset: the count stops before this number</param>
		private void AssertInState(Service.STATE expected, TestCompositeService.CompositeServiceImpl
			[] services, int start, int finish)
		{
			for (int i = start; i < finish; i++)
			{
				Org.Apache.Hadoop.Service.Service service = services[i];
				AssertInState(expected, service);
			}
		}

		private void AssertInState(Service.STATE expected, Org.Apache.Hadoop.Service.Service
			 service)
		{
			Assert.Equal("Service state should have been " + expected + " in "
				 + service, expected, service.GetServiceState());
		}

		/// <summary>Shut down from not-inited: expect nothing to have happened</summary>
		[Fact]
		public virtual void TestServiceStopFromNotInited()
		{
			TestCompositeService.ServiceManager serviceManager = new TestCompositeService.ServiceManager
				("ServiceManager");
			// Add services
			for (int i = 0; i < NumOfServices; i++)
			{
				TestCompositeService.CompositeServiceImpl service = new TestCompositeService.CompositeServiceImpl
					(i);
				serviceManager.AddTestService(service);
			}
			TestCompositeService.CompositeServiceImpl[] services = Collections.ToArray
				(serviceManager.GetServices(), new TestCompositeService.CompositeServiceImpl[0]);
			serviceManager.Stop();
			AssertInState(Service.STATE.Notinited, services);
		}

		/// <summary>Shut down from inited</summary>
		[Fact]
		public virtual void TestServiceStopFromInited()
		{
			TestCompositeService.ServiceManager serviceManager = new TestCompositeService.ServiceManager
				("ServiceManager");
			// Add services
			for (int i = 0; i < NumOfServices; i++)
			{
				TestCompositeService.CompositeServiceImpl service = new TestCompositeService.CompositeServiceImpl
					(i);
				serviceManager.AddTestService(service);
			}
			TestCompositeService.CompositeServiceImpl[] services = Collections.ToArray
				(serviceManager.GetServices(), new TestCompositeService.CompositeServiceImpl[0]);
			serviceManager.Init(new Configuration());
			serviceManager.Stop();
			if (StopOnlyStartedServices)
			{
				//this policy => no services were stopped
				AssertInState(Service.STATE.Inited, services);
			}
			else
			{
				AssertInState(Service.STATE.Stopped, services);
			}
		}

		/// <summary>Use a null configuration & expect a failure</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInitNullConf()
		{
			TestCompositeService.ServiceManager serviceManager = new TestCompositeService.ServiceManager
				("testInitNullConf");
			TestCompositeService.CompositeServiceImpl service = new TestCompositeService.CompositeServiceImpl
				(0);
			serviceManager.AddTestService(service);
			try
			{
				serviceManager.Init(null);
				Log.Warn("Null Configurations are permitted " + serviceManager);
			}
			catch (ServiceStateException)
			{
			}
		}

		//expected
		/// <summary>
		/// Walk the service through their lifecycle without any children;
		/// verify that it all works.
		/// </summary>
		[Fact]
		public virtual void TestServiceLifecycleNoChildren()
		{
			TestCompositeService.ServiceManager serviceManager = new TestCompositeService.ServiceManager
				("ServiceManager");
			serviceManager.Init(new Configuration());
			serviceManager.Start();
			serviceManager.Stop();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAddServiceInInit()
		{
			BreakableService child = new BreakableService();
			AssertInState(Service.STATE.Notinited, child);
			TestCompositeService.CompositeServiceAddingAChild composite = new TestCompositeService.CompositeServiceAddingAChild
				(child);
			composite.Init(new Configuration());
			AssertInState(Service.STATE.Inited, child);
		}

		public virtual void TestAddIfService()
		{
			CompositeService testService = new _CompositeService_317("TestService");
			testService.Init(new Configuration());
			Assert.Equal("Incorrect number of services", 1, testService.GetServices
				().Count);
		}

		private sealed class _CompositeService_317 : CompositeService
		{
			public _CompositeService_317(string baseArg1)
				: base(baseArg1)
			{
			}

			internal Org.Apache.Hadoop.Service.Service service;

			protected internal override void ServiceInit(Configuration conf)
			{
				int notAService = 0;
				NUnit.Framework.Assert.IsFalse("Added an integer as a service", this.AddIfService
					(notAService));
				this.service = new _AbstractService_325("Service");
				Assert.True("Unable to add a service", this.AddIfService(this.service
					));
			}

			private sealed class _AbstractService_325 : AbstractService
			{
				public _AbstractService_325(string baseArg1)
					: base(baseArg1)
				{
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddInitedSiblingInInit()
		{
			CompositeService parent = new CompositeService("parent");
			BreakableService sibling = new BreakableService();
			sibling.Init(new Configuration());
			parent.AddService(new TestCompositeService.AddSiblingService(parent, sibling, Service.STATE
				.Inited));
			parent.Init(new Configuration());
			parent.Start();
			parent.Stop();
			Assert.Equal("Incorrect number of services", 2, parent.GetServices
				().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddUninitedSiblingInInit()
		{
			CompositeService parent = new CompositeService("parent");
			BreakableService sibling = new BreakableService();
			parent.AddService(new TestCompositeService.AddSiblingService(parent, sibling, Service.STATE
				.Inited));
			parent.Init(new Configuration());
			try
			{
				parent.Start();
				NUnit.Framework.Assert.Fail("Expected an exception, got " + parent);
			}
			catch (ServiceStateException)
			{
			}
			//expected
			parent.Stop();
			Assert.Equal("Incorrect number of services", 2, parent.GetServices
				().Count);
		}

		[Fact]
		public virtual void TestRemoveService()
		{
			CompositeService testService = new _CompositeService_371("TestService");
			testService.Init(new Configuration());
			Assert.Equal("Incorrect number of services", 2, testService.GetServices
				().Count);
		}

		private sealed class _CompositeService_371 : CompositeService
		{
			public _CompositeService_371(string baseArg1)
				: base(baseArg1)
			{
			}

			protected internal override void ServiceInit(Configuration conf)
			{
				int notAService = 0;
				NUnit.Framework.Assert.IsFalse("Added an integer as a service", this.AddIfService
					(notAService));
				Org.Apache.Hadoop.Service.Service service1 = new _AbstractService_378("Service1");
				this.AddIfService(service1);
				Org.Apache.Hadoop.Service.Service service2 = new _AbstractService_381("Service2");
				this.AddIfService(service2);
				Org.Apache.Hadoop.Service.Service service3 = new _AbstractService_384("Service3");
				this.AddIfService(service3);
				this.RemoveService(service1);
			}

			private sealed class _AbstractService_378 : AbstractService
			{
				public _AbstractService_378(string baseArg1)
					: base(baseArg1)
				{
				}
			}

			private sealed class _AbstractService_381 : AbstractService
			{
				public _AbstractService_381(string baseArg1)
					: base(baseArg1)
				{
				}
			}

			private sealed class _AbstractService_384 : AbstractService
			{
				public _AbstractService_384(string baseArg1)
					: base(baseArg1)
				{
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddStartedChildBeforeInit()
		{
			CompositeService parent = new CompositeService("parent");
			BreakableService child = new BreakableService();
			child.Init(new Configuration());
			child.Start();
			TestCompositeService.AddSiblingService.AddChildToService(parent, child);
			try
			{
				parent.Init(new Configuration());
				NUnit.Framework.Assert.Fail("Expected an exception, got " + parent);
			}
			catch (ServiceStateException)
			{
			}
			//expected
			parent.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddStoppedChildBeforeInit()
		{
			CompositeService parent = new CompositeService("parent");
			BreakableService child = new BreakableService();
			child.Init(new Configuration());
			child.Start();
			child.Stop();
			TestCompositeService.AddSiblingService.AddChildToService(parent, child);
			try
			{
				parent.Init(new Configuration());
				NUnit.Framework.Assert.Fail("Expected an exception, got " + parent);
			}
			catch (ServiceStateException)
			{
			}
			//expected
			parent.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddStartedSiblingInStart()
		{
			CompositeService parent = new CompositeService("parent");
			BreakableService sibling = new BreakableService();
			sibling.Init(new Configuration());
			sibling.Start();
			parent.AddService(new TestCompositeService.AddSiblingService(parent, sibling, Service.STATE
				.Started));
			parent.Init(new Configuration());
			parent.Start();
			parent.Stop();
			Assert.Equal("Incorrect number of services", 2, parent.GetServices
				().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddUninitedSiblingInStart()
		{
			CompositeService parent = new CompositeService("parent");
			BreakableService sibling = new BreakableService();
			parent.AddService(new TestCompositeService.AddSiblingService(parent, sibling, Service.STATE
				.Started));
			parent.Init(new Configuration());
			AssertInState(Service.STATE.Notinited, sibling);
			parent.Start();
			parent.Stop();
			Assert.Equal("Incorrect number of services", 2, parent.GetServices
				().Count);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddStartedSiblingInInit()
		{
			CompositeService parent = new CompositeService("parent");
			BreakableService sibling = new BreakableService();
			sibling.Init(new Configuration());
			sibling.Start();
			parent.AddService(new TestCompositeService.AddSiblingService(parent, sibling, Service.STATE
				.Inited));
			parent.Init(new Configuration());
			AssertInState(Service.STATE.Started, sibling);
			parent.Start();
			AssertInState(Service.STATE.Started, sibling);
			parent.Stop();
			Assert.Equal("Incorrect number of services", 2, parent.GetServices
				().Count);
			AssertInState(Service.STATE.Stopped, sibling);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAddStartedSiblingInStop()
		{
			CompositeService parent = new CompositeService("parent");
			BreakableService sibling = new BreakableService();
			sibling.Init(new Configuration());
			sibling.Start();
			parent.AddService(new TestCompositeService.AddSiblingService(parent, sibling, Service.STATE
				.Stopped));
			parent.Init(new Configuration());
			parent.Start();
			parent.Stop();
			Assert.Equal("Incorrect number of services", 2, parent.GetServices
				().Count);
		}

		public class CompositeServiceAddingAChild : CompositeService
		{
			internal Org.Apache.Hadoop.Service.Service child;

			public CompositeServiceAddingAChild(Org.Apache.Hadoop.Service.Service child)
				: base("CompositeServiceAddingAChild")
			{
				this.child = child;
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceInit(Configuration conf)
			{
				AddService(child);
				base.ServiceInit(conf);
			}
		}

		[System.Serializable]
		public class ServiceTestRuntimeException : RuntimeException
		{
			public ServiceTestRuntimeException(string message)
				: base(message)
			{
			}
		}

		/// <summary>
		/// This is a composite service that keeps a count of the number of lifecycle
		/// events called, and can be set to throw a
		/// <see cref="ServiceTestRuntimeException"></see>
		/// during service start or stop
		/// </summary>
		public class CompositeServiceImpl : CompositeService
		{
			public static bool IsPolicyToStopOnlyStartedServices()
			{
				return StopOnlyStartedServices;
			}

			private static int counter = -1;

			private int callSequenceNumber = -1;

			private bool throwExceptionOnStart;

			private bool throwExceptionOnStop;

			public CompositeServiceImpl(int sequenceNumber)
				: base(Extensions.ToString(sequenceNumber))
			{
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceInit(Configuration conf)
			{
				counter++;
				callSequenceNumber = counter;
				base.ServiceInit(conf);
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceStart()
			{
				if (throwExceptionOnStart)
				{
					throw new TestCompositeService.ServiceTestRuntimeException("Fake service start exception"
						);
				}
				counter++;
				callSequenceNumber = counter;
				base.ServiceStart();
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceStop()
			{
				counter++;
				callSequenceNumber = counter;
				if (throwExceptionOnStop)
				{
					throw new TestCompositeService.ServiceTestRuntimeException("Fake service stop exception"
						);
				}
				base.ServiceStop();
			}

			public static int GetCounter()
			{
				return counter;
			}

			public virtual int GetCallSequenceNumber()
			{
				return callSequenceNumber;
			}

			public virtual void Reset()
			{
				callSequenceNumber = -1;
				counter = -1;
			}

			public static void ResetCounter()
			{
				counter = -1;
			}

			public virtual void SetThrowExceptionOnStart(bool throwExceptionOnStart)
			{
				this.throwExceptionOnStart = throwExceptionOnStart;
			}

			public virtual void SetThrowExceptionOnStop(bool throwExceptionOnStop)
			{
				this.throwExceptionOnStop = throwExceptionOnStop;
			}

			public override string ToString()
			{
				return "Service " + GetName();
			}
		}

		/// <summary>Composite service that makes the addService method public to all</summary>
		public class ServiceManager : CompositeService
		{
			public virtual void AddTestService(CompositeService service)
			{
				AddService(service);
			}

			public ServiceManager(string name)
				: base(name)
			{
			}
		}

		public class AddSiblingService : CompositeService
		{
			private readonly CompositeService parent;

			private readonly Org.Apache.Hadoop.Service.Service serviceToAdd;

			private Service.STATE triggerState;

			public AddSiblingService(CompositeService parent, Org.Apache.Hadoop.Service.Service
				 serviceToAdd, Service.STATE triggerState)
				: base("ParentStateManipulatorService")
			{
				this.parent = parent;
				this.serviceToAdd = serviceToAdd;
				this.triggerState = triggerState;
			}

			/// <summary>
			/// Add the serviceToAdd to the parent if this service
			/// is in the state requested
			/// </summary>
			private void MaybeAddSibling()
			{
				if (GetServiceState() == triggerState)
				{
					parent.AddService(serviceToAdd);
				}
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceInit(Configuration conf)
			{
				MaybeAddSibling();
				base.ServiceInit(conf);
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceStart()
			{
				MaybeAddSibling();
				base.ServiceStart();
			}

			/// <exception cref="System.Exception"/>
			protected internal override void ServiceStop()
			{
				MaybeAddSibling();
				base.ServiceStop();
			}

			/// <summary>Expose addService method</summary>
			/// <param name="parent">parent service</param>
			/// <param name="child">child to add</param>
			public static void AddChildToService(CompositeService parent, Org.Apache.Hadoop.Service.Service
				 child)
			{
				parent.AddService(child);
			}
		}
	}
}
