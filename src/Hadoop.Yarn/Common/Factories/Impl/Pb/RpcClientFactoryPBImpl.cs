using System;
using System.Net;
using System.Reflection;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Factories.Impl.PB
{
	public class RpcClientFactoryPBImpl : RpcClientFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcClientFactoryPBImpl
			));

		private const string PbImplPackageSuffix = "impl.pb.client";

		private const string PbImplClassSuffix = "PBClientImpl";

		private static readonly Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcClientFactoryPBImpl
			 self = new Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcClientFactoryPBImpl();

		private Configuration localConf = new Configuration();

		private ConcurrentMap<Type, Constructor<object>> cache = new ConcurrentHashMap<Type
			, Constructor<object>>();

		public static Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcClientFactoryPBImpl Get
			()
		{
			return Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcClientFactoryPBImpl.self;
		}

		private RpcClientFactoryPBImpl()
		{
		}

		public virtual object GetClient(Type protocol, long clientVersion, IPEndPoint addr
			, Configuration conf)
		{
			Constructor<object> constructor = cache[protocol];
			if (constructor == null)
			{
				Type pbClazz = null;
				try
				{
					pbClazz = localConf.GetClassByName(GetPBImplClassName(protocol));
				}
				catch (TypeLoadException e)
				{
					throw new YarnRuntimeException("Failed to load class: [" + GetPBImplClassName(protocol
						) + "]", e);
				}
				try
				{
					constructor = pbClazz.GetConstructor(typeof(long), typeof(IPEndPoint), typeof(Configuration
						));
					cache.PutIfAbsent(protocol, constructor);
				}
				catch (MissingMethodException e)
				{
					throw new YarnRuntimeException("Could not find constructor with params: " + typeof(
						long) + ", " + typeof(IPEndPoint) + ", " + typeof(Configuration), e);
				}
			}
			try
			{
				object retObject = constructor.NewInstance(clientVersion, addr, conf);
				return retObject;
			}
			catch (TargetInvocationException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MemberAccessException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (InstantiationException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		public virtual void StopClient(object proxy)
		{
			try
			{
				if (proxy is IDisposable)
				{
					((IDisposable)proxy).Close();
					return;
				}
				else
				{
					InvocationHandler handler = Proxy.GetInvocationHandler(proxy);
					if (handler is IDisposable)
					{
						((IDisposable)handler).Close();
						return;
					}
				}
			}
			catch (Exception e)
			{
				Log.Error("Cannot call close method due to Exception. " + "Ignoring.", e);
				throw new YarnRuntimeException(e);
			}
			throw new HadoopIllegalArgumentException("Cannot close proxy - is not Closeable or "
				 + "does not provide closeable invocation handler " + proxy.GetType());
		}

		private string GetPBImplClassName(Type clazz)
		{
			string srcPackagePart = GetPackageName(clazz);
			string srcClassName = GetClassName(clazz);
			string destPackagePart = srcPackagePart + "." + PbImplPackageSuffix;
			string destClassPart = srcClassName + PbImplClassSuffix;
			return destPackagePart + "." + destClassPart;
		}

		private string GetClassName(Type clazz)
		{
			string fqName = clazz.FullName;
			return (Sharpen.Runtime.Substring(fqName, fqName.LastIndexOf(".") + 1, fqName.Length
				));
		}

		private string GetPackageName(Type clazz)
		{
			return clazz.Assembly.GetName();
		}
	}
}
