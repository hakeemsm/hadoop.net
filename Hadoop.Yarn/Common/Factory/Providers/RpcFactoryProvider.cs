using System;
using System.Reflection;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Factory.Providers
{
	/// <summary>A public static get() method must be present in the Client/Server Factory implementation.
	/// 	</summary>
	public class RpcFactoryProvider
	{
		private RpcFactoryProvider()
		{
		}

		public static RpcServerFactory GetServerFactory(Configuration conf)
		{
			if (conf == null)
			{
				conf = new Configuration();
			}
			string serverFactoryClassName = conf.Get(YarnConfiguration.IpcServerFactoryClass, 
				YarnConfiguration.DefaultIpcServerFactoryClass);
			return (RpcServerFactory)GetFactoryClassInstance(serverFactoryClassName);
		}

		public static RpcClientFactory GetClientFactory(Configuration conf)
		{
			string clientFactoryClassName = conf.Get(YarnConfiguration.IpcClientFactoryClass, 
				YarnConfiguration.DefaultIpcClientFactoryClass);
			return (RpcClientFactory)GetFactoryClassInstance(clientFactoryClassName);
		}

		private static object GetFactoryClassInstance(string factoryClassName)
		{
			try
			{
				Type clazz = Sharpen.Runtime.GetType(factoryClassName);
				MethodInfo method = clazz.GetMethod("get", null);
				return method.Invoke(null, null);
			}
			catch (TypeLoadException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MissingMethodException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (TargetInvocationException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MemberAccessException e)
			{
				throw new YarnRuntimeException(e);
			}
		}
	}
}
