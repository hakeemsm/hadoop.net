using System;
using System.IO;
using System.Net;
using System.Reflection;
using Com.Google.Protobuf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Yarn.Factories.Impl.PB
{
	public class RpcServerFactoryPBImpl : RpcServerFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcServerFactoryPBImpl
			));

		private const string ProtoGenPackageName = "org.apache.hadoop.yarn.proto";

		private const string ProtoGenClassSuffix = "Service";

		private const string PbImplPackageSuffix = "impl.pb.service";

		private const string PbImplClassSuffix = "PBServiceImpl";

		private static readonly Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcServerFactoryPBImpl
			 self = new Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcServerFactoryPBImpl();

		private Configuration localConf = new Configuration();

		private ConcurrentMap<Type, Constructor<object>> serviceCache = new ConcurrentHashMap
			<Type, Constructor<object>>();

		private ConcurrentMap<Type, MethodInfo> protoCache = new ConcurrentHashMap<Type, 
			MethodInfo>();

		public static Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcServerFactoryPBImpl Get
			()
		{
			return Org.Apache.Hadoop.Yarn.Factories.Impl.PB.RpcServerFactoryPBImpl.self;
		}

		private RpcServerFactoryPBImpl()
		{
		}

		public virtual Server GetServer<_T0>(Type protocol, object instance, IPEndPoint addr
			, Configuration conf, SecretManager<_T0> secretManager, int numHandlers)
			where _T0 : TokenIdentifier
		{
			return GetServer(protocol, instance, addr, conf, secretManager, numHandlers, null
				);
		}

		public virtual Server GetServer<_T0>(Type protocol, object instance, IPEndPoint addr
			, Configuration conf, SecretManager<_T0> secretManager, int numHandlers, string 
			portRangeConfig)
			where _T0 : TokenIdentifier
		{
			Constructor<object> constructor = serviceCache[protocol];
			if (constructor == null)
			{
				Type pbServiceImplClazz = null;
				try
				{
					pbServiceImplClazz = localConf.GetClassByName(GetPbServiceImplClassName(protocol)
						);
				}
				catch (TypeLoadException e)
				{
					throw new YarnRuntimeException("Failed to load class: [" + GetPbServiceImplClassName
						(protocol) + "]", e);
				}
				try
				{
					constructor = pbServiceImplClazz.GetConstructor(protocol);
					serviceCache.PutIfAbsent(protocol, constructor);
				}
				catch (MissingMethodException e)
				{
					throw new YarnRuntimeException("Could not find constructor with params: " + typeof(
						long) + ", " + typeof(IPEndPoint) + ", " + typeof(Configuration), e);
				}
			}
			object service = null;
			try
			{
				service = constructor.NewInstance(instance);
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
			Type pbProtocol = service.GetType().GetInterfaces()[0];
			MethodInfo method = protoCache[protocol];
			if (method == null)
			{
				Type protoClazz = null;
				try
				{
					protoClazz = localConf.GetClassByName(GetProtoClassName(protocol));
				}
				catch (TypeLoadException e)
				{
					throw new YarnRuntimeException("Failed to load class: [" + GetProtoClassName(protocol
						) + "]", e);
				}
				try
				{
					method = protoClazz.GetMethod("newReflectiveBlockingService", pbProtocol.GetInterfaces
						()[0]);
					protoCache.PutIfAbsent(protocol, method);
				}
				catch (MissingMethodException e)
				{
					throw new YarnRuntimeException(e);
				}
			}
			try
			{
				return CreateServer(pbProtocol, addr, conf, secretManager, numHandlers, (BlockingService
					)method.Invoke(null, service), portRangeConfig);
			}
			catch (TargetInvocationException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (MemberAccessException e)
			{
				throw new YarnRuntimeException(e);
			}
			catch (IOException e)
			{
				throw new YarnRuntimeException(e);
			}
		}

		private string GetProtoClassName(Type clazz)
		{
			string srcClassName = GetClassName(clazz);
			return ProtoGenPackageName + "." + srcClassName + "$" + srcClassName + ProtoGenClassSuffix;
		}

		private string GetPbServiceImplClassName(Type clazz)
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

		/// <exception cref="System.IO.IOException"/>
		private Server CreateServer<_T0>(Type pbProtocol, IPEndPoint addr, Configuration 
			conf, SecretManager<_T0> secretManager, int numHandlers, BlockingService blockingService
			, string portRangeConfig)
			where _T0 : TokenIdentifier
		{
			RPC.SetProtocolEngine(conf, pbProtocol, typeof(ProtobufRpcEngine));
			RPC.Server server = new RPC.Builder(conf).SetProtocol(pbProtocol).SetInstance(blockingService
				).SetBindAddress(addr.GetHostName()).SetPort(addr.Port).SetNumHandlers(numHandlers
				).SetVerbose(false).SetSecretManager(secretManager).SetPortRangeConfig(portRangeConfig
				).Build();
			Log.Info("Adding protocol " + pbProtocol.GetCanonicalName() + " to the server");
			server.AddProtocol(RPC.RpcKind.RpcProtocolBuffer, pbProtocol, blockingService);
			return server;
		}
	}
}
