using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class TestRpcFactoryProvider
	{
		[NUnit.Framework.Test]
		public virtual void TestFactoryProvider()
		{
			Configuration conf = new Configuration();
			RpcClientFactory clientFactory = null;
			RpcServerFactory serverFactory = null;
			clientFactory = RpcFactoryProvider.GetClientFactory(conf);
			serverFactory = RpcFactoryProvider.GetServerFactory(conf);
			NUnit.Framework.Assert.AreEqual(typeof(RpcClientFactoryPBImpl), clientFactory.GetType
				());
			NUnit.Framework.Assert.AreEqual(typeof(RpcServerFactoryPBImpl), serverFactory.GetType
				());
			conf.Set(YarnConfiguration.IpcClientFactoryClass, "unknown");
			conf.Set(YarnConfiguration.IpcServerFactoryClass, "unknown");
			conf.Set(YarnConfiguration.IpcRecordFactoryClass, "unknown");
			try
			{
				clientFactory = RpcFactoryProvider.GetClientFactory(conf);
				NUnit.Framework.Assert.Fail("Expected an exception - unknown serializer");
			}
			catch (YarnRuntimeException)
			{
			}
			try
			{
				serverFactory = RpcFactoryProvider.GetServerFactory(conf);
				NUnit.Framework.Assert.Fail("Expected an exception - unknown serializer");
			}
			catch (YarnRuntimeException)
			{
			}
			conf = new Configuration();
			conf.Set(YarnConfiguration.IpcClientFactoryClass, "NonExistantClass");
			conf.Set(YarnConfiguration.IpcServerFactoryClass, typeof(RpcServerFactoryPBImpl).
				FullName);
			try
			{
				clientFactory = RpcFactoryProvider.GetClientFactory(conf);
				NUnit.Framework.Assert.Fail("Expected an exception - unknown class");
			}
			catch (YarnRuntimeException)
			{
			}
			try
			{
				serverFactory = RpcFactoryProvider.GetServerFactory(conf);
			}
			catch (YarnRuntimeException)
			{
				NUnit.Framework.Assert.Fail("Error while loading factory using reflection: [" + typeof(
					RpcServerFactoryPBImpl).FullName + "]");
			}
		}
	}
}
