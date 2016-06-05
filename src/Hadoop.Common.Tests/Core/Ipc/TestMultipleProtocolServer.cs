using System;
using System.IO;
using System.Net;
using Com.Google.Protobuf;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Net;


namespace Org.Apache.Hadoop.Ipc
{
	public class TestMultipleProtocolServer
	{
		private const string Address = "0.0.0.0";

		private static IPEndPoint addr;

		private static RPC.Server server;

		private static Configuration conf = new Configuration();

		internal abstract class Foo0 : VersionedProtocol
		{
			public const long versionID = 0L;

			/// <exception cref="System.IO.IOException"/>
			public abstract string Ping();
		}

		internal static class Foo0Constants
		{
		}

		internal abstract class Foo1 : VersionedProtocol
		{
			public const long versionID = 1L;

			/// <exception cref="System.IO.IOException"/>
			public abstract string Ping();

			/// <exception cref="System.IO.IOException"/>
			public abstract string Ping2();
		}

		internal static class Foo1Constants
		{
		}

		internal abstract class FooUnimplemented : VersionedProtocol
		{
			public const long versionID = 2L;

			/// <exception cref="System.IO.IOException"/>
			public abstract string Ping();
		}

		internal static class FooUnimplementedConstants
		{
		}

		internal abstract class Mixin : VersionedProtocol
		{
			public const long versionID = 0L;

			/// <exception cref="System.IO.IOException"/>
			public abstract void Hello();
		}

		internal static class MixinConstants
		{
		}

		internal abstract class Bar : TestMultipleProtocolServer.Mixin
		{
			public const long versionID = 0L;

			/// <exception cref="System.IO.IOException"/>
			public abstract int Echo(int i);
		}

		internal static class BarConstants
		{
		}

		internal class Foo0Impl : TestMultipleProtocolServer.Foo0
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TestMultipleProtocolServer.Foo0.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHash)
			{
				Type inter;
				try
				{
					inter = (Type)this.GetType().GetGenericInterfaces()[0];
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				return ProtocolSignature.GetProtocolSignature(clientMethodsHash, this.GetProtocolVersion
					(protocol, clientVersion), inter);
			}

			public override string Ping()
			{
				return "Foo0";
			}

			internal Foo0Impl(TestMultipleProtocolServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMultipleProtocolServer _enclosing;
		}

		internal class Foo1Impl : TestMultipleProtocolServer.Foo1
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TestMultipleProtocolServer.Foo1.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHash)
			{
				Type inter;
				try
				{
					inter = (Type)this.GetType().GetGenericInterfaces()[0];
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				return ProtocolSignature.GetProtocolSignature(clientMethodsHash, this.GetProtocolVersion
					(protocol, clientVersion), inter);
			}

			public override string Ping()
			{
				return "Foo1";
			}

			public override string Ping2()
			{
				return "Foo1";
			}

			internal Foo1Impl(TestMultipleProtocolServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMultipleProtocolServer _enclosing;
		}

		internal class BarImpl : TestMultipleProtocolServer.Bar
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TestMultipleProtocolServer.Bar.versionID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int clientMethodsHash)
			{
				Type inter;
				try
				{
					inter = (Type)this.GetType().GetGenericInterfaces()[0];
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				return ProtocolSignature.GetProtocolSignature(clientMethodsHash, this.GetProtocolVersion
					(protocol, clientVersion), inter);
			}

			public override int Echo(int i)
			{
				return i;
			}

			public virtual void Hello()
			{
			}

			internal BarImpl(TestMultipleProtocolServer _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestMultipleProtocolServer _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetUp()
		{
			// create a server with two handlers
			server = new RPC.Builder(conf).SetProtocol(typeof(TestMultipleProtocolServer.Foo0
				)).SetInstance(new TestMultipleProtocolServer.Foo0Impl(this)).SetBindAddress(Address
				).SetPort(0).SetNumHandlers(2).SetVerbose(false).Build();
			server.AddProtocol(RPC.RpcKind.RpcWritable, typeof(TestMultipleProtocolServer.Foo1
				), new TestMultipleProtocolServer.Foo1Impl(this));
			server.AddProtocol(RPC.RpcKind.RpcWritable, typeof(TestMultipleProtocolServer.Bar
				), new TestMultipleProtocolServer.BarImpl(this));
			server.AddProtocol(RPC.RpcKind.RpcWritable, typeof(TestMultipleProtocolServer.Mixin
				), new TestMultipleProtocolServer.BarImpl(this));
			// Add Protobuf server
			// Create server side implementation
			TestProtoBufRpc.PBServerImpl pbServerImpl = new TestProtoBufRpc.PBServerImpl();
			BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto.NewReflectiveBlockingService
				(pbServerImpl);
			server.AddProtocol(RPC.RpcKind.RpcProtocolBuffer, typeof(TestProtoBufRpc.TestRpcService
				), service);
			server.Start();
			addr = NetUtils.GetConnectAddress(server);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			server.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void Test1()
		{
			ProtocolProxy<object> proxy;
			proxy = RPC.GetProtocolProxy<TestMultipleProtocolServer.Foo0>(TestMultipleProtocolServer.Foo0
				.versionID, addr, conf);
			TestMultipleProtocolServer.Foo0 foo0 = (TestMultipleProtocolServer.Foo0)proxy.GetProxy
				();
			Assert.Equal("Foo0", foo0.Ping());
			proxy = RPC.GetProtocolProxy<TestMultipleProtocolServer.Foo1>(TestMultipleProtocolServer.Foo1
				.versionID, addr, conf);
			TestMultipleProtocolServer.Foo1 foo1 = (TestMultipleProtocolServer.Foo1)proxy.GetProxy
				();
			Assert.Equal("Foo1", foo1.Ping());
			Assert.Equal("Foo1", foo1.Ping());
			proxy = RPC.GetProtocolProxy<TestMultipleProtocolServer.Bar>(TestMultipleProtocolServer.Foo1
				.versionID, addr, conf);
			TestMultipleProtocolServer.Bar bar = (TestMultipleProtocolServer.Bar)proxy.GetProxy
				();
			Assert.Equal(99, bar.Echo(99));
			// Now test Mixin class method
			TestMultipleProtocolServer.Mixin mixin = bar;
			mixin.Hello();
		}

		// Server does not implement the FooUnimplemented version of protocol Foo.
		// See that calls to it fail.
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNonExistingProtocol()
		{
			ProtocolProxy<object> proxy;
			proxy = RPC.GetProtocolProxy<TestMultipleProtocolServer.FooUnimplemented>(TestMultipleProtocolServer.FooUnimplemented
				.versionID, addr, conf);
			TestMultipleProtocolServer.FooUnimplemented foo = (TestMultipleProtocolServer.FooUnimplemented
				)proxy.GetProxy();
			foo.Ping();
		}

		/// <summary>
		/// getProtocolVersion of an unimplemented version should return highest version
		/// Similarly getProtocolSignature should work.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestNonExistingProtocol2()
		{
			ProtocolProxy<object> proxy;
			proxy = RPC.GetProtocolProxy<TestMultipleProtocolServer.FooUnimplemented>(TestMultipleProtocolServer.FooUnimplemented
				.versionID, addr, conf);
			TestMultipleProtocolServer.FooUnimplemented foo = (TestMultipleProtocolServer.FooUnimplemented
				)proxy.GetProxy();
			Assert.Equal(TestMultipleProtocolServer.Foo1.versionID, foo.GetProtocolVersion
				(RPC.GetProtocolName(typeof(TestMultipleProtocolServer.FooUnimplemented)), TestMultipleProtocolServer.FooUnimplemented
				.versionID));
			foo.GetProtocolSignature(RPC.GetProtocolName(typeof(TestMultipleProtocolServer.FooUnimplemented
				)), TestMultipleProtocolServer.FooUnimplemented.versionID, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIncorrectServerCreation()
		{
			new RPC.Builder(conf).SetProtocol(typeof(TestMultipleProtocolServer.Foo1)).SetInstance
				(new TestMultipleProtocolServer.Foo0Impl(this)).SetBindAddress(Address).SetPort(
				0).SetNumHandlers(2).SetVerbose(false).Build();
		}

		// Now test a PB service - a server  hosts both PB and Writable Rpcs.
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestPBService()
		{
			// Set RPC engine to protobuf RPC engine
			Configuration conf2 = new Configuration();
			RPC.SetProtocolEngine(conf2, typeof(TestProtoBufRpc.TestRpcService), typeof(ProtobufRpcEngine
				));
			TestProtoBufRpc.TestRpcService client = RPC.GetProxy<TestProtoBufRpc.TestRpcService
				>(0, addr, conf2);
			TestProtoBufRpc.TestProtoBufRpc(client);
		}
	}
}
