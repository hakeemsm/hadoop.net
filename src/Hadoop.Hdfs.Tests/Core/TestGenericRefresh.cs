using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop
{
	/// <summary>Before all tests, a MiniDFSCluster is spun up.</summary>
	/// <remarks>
	/// Before all tests, a MiniDFSCluster is spun up.
	/// Before each test, mock refresh handlers are created and registered.
	/// After each test, the mock handlers are unregistered.
	/// After all tests, the cluster is spun down.
	/// </remarks>
	public class TestGenericRefresh
	{
		private static MiniDFSCluster cluster;

		private static Configuration config;

		private static RefreshHandler firstHandler;

		private static RefreshHandler secondHandler;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUpBeforeClass()
		{
			config = new Configuration();
			config.Set("hadoop.security.authorization", "true");
			FileSystem.SetDefaultUri(config, "hdfs://localhost:0");
			cluster = new MiniDFSCluster.Builder(config).Build();
			cluster.WaitActive();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDownBeforeClass()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			// Register Handlers, first one just sends an ok response
			firstHandler = Org.Mockito.Mockito.Mock<RefreshHandler>();
			Org.Mockito.Mockito.Stub(firstHandler.HandleRefresh(Org.Mockito.Mockito.AnyString
				(), Org.Mockito.Mockito.Any<string[]>())).ToReturn(RefreshResponse.SuccessResponse
				());
			RefreshRegistry.DefaultRegistry().Register("firstHandler", firstHandler);
			// Second handler has conditional response for testing args
			secondHandler = Org.Mockito.Mockito.Mock<RefreshHandler>();
			Org.Mockito.Mockito.Stub(secondHandler.HandleRefresh("secondHandler", new string[
				] { "one", "two" })).ToReturn(new RefreshResponse(3, "three"));
			Org.Mockito.Mockito.Stub(secondHandler.HandleRefresh("secondHandler", new string[
				] { "one" })).ToReturn(new RefreshResponse(2, "two"));
			RefreshRegistry.DefaultRegistry().Register("secondHandler", secondHandler);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			RefreshRegistry.DefaultRegistry().UnregisterAll("firstHandler");
			RefreshRegistry.DefaultRegistry().UnregisterAll("secondHandler");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidCommand()
		{
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refresh", "nn" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual("DFSAdmin should fail due to bad args", -1, exitCode
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidIdentifier()
		{
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refresh", "localhost:" + cluster.GetNameNodePort
				(), "unregisteredIdentity" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual("DFSAdmin should fail due to no handler registered"
				, -1, exitCode);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestValidIdentifier()
		{
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refresh", "localhost:" + cluster.GetNameNodePort
				(), "firstHandler" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual("DFSAdmin should succeed", 0, exitCode);
			Org.Mockito.Mockito.Verify(firstHandler).HandleRefresh("firstHandler", new string
				[] {  });
			// Second handler was never called
			Org.Mockito.Mockito.Verify(secondHandler, Org.Mockito.Mockito.Never()).HandleRefresh
				(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito.Any<string[]>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVariableArgs()
		{
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refresh", "localhost:" + cluster.GetNameNodePort
				(), "secondHandler", "one" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual("DFSAdmin should return 2", 2, exitCode);
			exitCode = admin.Run(new string[] { "-refresh", "localhost:" + cluster.GetNameNodePort
				(), "secondHandler", "one", "two" });
			NUnit.Framework.Assert.AreEqual("DFSAdmin should now return 3", 3, exitCode);
			Org.Mockito.Mockito.Verify(secondHandler).HandleRefresh("secondHandler", new string
				[] { "one" });
			Org.Mockito.Mockito.Verify(secondHandler).HandleRefresh("secondHandler", new string
				[] { "one", "two" });
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnregistration()
		{
			RefreshRegistry.DefaultRegistry().UnregisterAll("firstHandler");
			// And now this should fail
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refresh", "localhost:" + cluster.GetNameNodePort
				(), "firstHandler" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual("DFSAdmin should return -1", -1, exitCode);
		}

		[NUnit.Framework.Test]
		public virtual void TestUnregistrationReturnValue()
		{
			RefreshHandler mockHandler = Org.Mockito.Mockito.Mock<RefreshHandler>();
			RefreshRegistry.DefaultRegistry().Register("test", mockHandler);
			bool ret = RefreshRegistry.DefaultRegistry().Unregister("test", mockHandler);
			NUnit.Framework.Assert.IsTrue(ret);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleRegistration()
		{
			RefreshRegistry.DefaultRegistry().Register("sharedId", firstHandler);
			RefreshRegistry.DefaultRegistry().Register("sharedId", secondHandler);
			// this should trigger both
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refresh", "localhost:" + cluster.GetNameNodePort
				(), "sharedId", "one" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual(-1, exitCode);
			// -1 because one of the responses is unregistered
			// verify we called both
			Org.Mockito.Mockito.Verify(firstHandler).HandleRefresh("sharedId", new string[] { 
				"one" });
			Org.Mockito.Mockito.Verify(secondHandler).HandleRefresh("sharedId", new string[] 
				{ "one" });
			RefreshRegistry.DefaultRegistry().UnregisterAll("sharedId");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleReturnCodeMerging()
		{
			// Two handlers which return two non-zero values
			RefreshHandler handlerOne = Org.Mockito.Mockito.Mock<RefreshHandler>();
			Org.Mockito.Mockito.Stub(handlerOne.HandleRefresh(Org.Mockito.Mockito.AnyString()
				, Org.Mockito.Mockito.Any<string[]>())).ToReturn(new RefreshResponse(23, "Twenty Three"
				));
			RefreshHandler handlerTwo = Org.Mockito.Mockito.Mock<RefreshHandler>();
			Org.Mockito.Mockito.Stub(handlerTwo.HandleRefresh(Org.Mockito.Mockito.AnyString()
				, Org.Mockito.Mockito.Any<string[]>())).ToReturn(new RefreshResponse(10, "Ten"));
			// Then registered to the same ID
			RefreshRegistry.DefaultRegistry().Register("shared", handlerOne);
			RefreshRegistry.DefaultRegistry().Register("shared", handlerTwo);
			// We refresh both
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refresh", "localhost:" + cluster.GetNameNodePort
				(), "shared" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual(-1, exitCode);
			// We get -1 because of our logic for melding non-zero return codes
			// Verify we called both
			Org.Mockito.Mockito.Verify(handlerOne).HandleRefresh("shared", new string[] {  });
			Org.Mockito.Mockito.Verify(handlerTwo).HandleRefresh("shared", new string[] {  });
			RefreshRegistry.DefaultRegistry().UnregisterAll("shared");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExceptionResultsInNormalError()
		{
			// In this test, we ensure that all handlers are called even if we throw an exception in one
			RefreshHandler exceptionalHandler = Org.Mockito.Mockito.Mock<RefreshHandler>();
			Org.Mockito.Mockito.Stub(exceptionalHandler.HandleRefresh(Org.Mockito.Mockito.AnyString
				(), Org.Mockito.Mockito.Any<string[]>())).ToThrow(new RuntimeException("Exceptional Handler Throws Exception"
				));
			RefreshHandler otherExceptionalHandler = Org.Mockito.Mockito.Mock<RefreshHandler>
				();
			Org.Mockito.Mockito.Stub(otherExceptionalHandler.HandleRefresh(Org.Mockito.Mockito
				.AnyString(), Org.Mockito.Mockito.Any<string[]>())).ToThrow(new RuntimeException
				("More Exceptions"));
			RefreshRegistry.DefaultRegistry().Register("exceptional", exceptionalHandler);
			RefreshRegistry.DefaultRegistry().Register("exceptional", otherExceptionalHandler
				);
			DFSAdmin admin = new DFSAdmin(config);
			string[] args = new string[] { "-refresh", "localhost:" + cluster.GetNameNodePort
				(), "exceptional" };
			int exitCode = admin.Run(args);
			NUnit.Framework.Assert.AreEqual(-1, exitCode);
			// Exceptions result in a -1
			Org.Mockito.Mockito.Verify(exceptionalHandler).HandleRefresh("exceptional", new string
				[] {  });
			Org.Mockito.Mockito.Verify(otherExceptionalHandler).HandleRefresh("exceptional", 
				new string[] {  });
			RefreshRegistry.DefaultRegistry().UnregisterAll("exceptional");
		}
	}
}
