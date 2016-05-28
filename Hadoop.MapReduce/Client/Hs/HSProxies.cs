using System;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.HS.ProtocolPB;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.ProtocolPB;
using Org.Apache.Hadoop.Tools;
using Org.Apache.Hadoop.Tools.ProtocolPB;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class HSProxies
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(HSProxies));

		/// <exception cref="System.IO.IOException"/>
		public static T CreateProxy<T>(Configuration conf, IPEndPoint hsaddr, UserGroupInformation
			 ugi)
		{
			System.Type xface = typeof(T);
			T proxy;
			if (xface == typeof(RefreshUserMappingsProtocol))
			{
				proxy = (T)CreateHSProxyWithRefreshUserMappingsProtocol(hsaddr, conf, ugi);
			}
			else
			{
				if (xface == typeof(GetUserMappingsProtocol))
				{
					proxy = (T)CreateHSProxyWithGetUserMappingsProtocol(hsaddr, conf, ugi);
				}
				else
				{
					if (xface == typeof(HSAdminRefreshProtocol))
					{
						proxy = (T)CreateHSProxyWithHSAdminRefreshProtocol(hsaddr, conf, ugi);
					}
					else
					{
						string message = "Unsupported protocol found when creating the proxy " + "connection to History server: "
							 + ((xface != null) ? xface.GetType().FullName : "null");
						Log.Error(message);
						throw new InvalidOperationException(message);
					}
				}
			}
			return proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		private static RefreshUserMappingsProtocol CreateHSProxyWithRefreshUserMappingsProtocol
			(IPEndPoint address, Configuration conf, UserGroupInformation ugi)
		{
			RefreshUserMappingsProtocolPB proxy = (RefreshUserMappingsProtocolPB)CreateHSProxy
				(address, conf, ugi, typeof(RefreshUserMappingsProtocolPB), 0);
			return new RefreshUserMappingsProtocolClientSideTranslatorPB(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		private static GetUserMappingsProtocol CreateHSProxyWithGetUserMappingsProtocol(IPEndPoint
			 address, Configuration conf, UserGroupInformation ugi)
		{
			GetUserMappingsProtocolPB proxy = (GetUserMappingsProtocolPB)CreateHSProxy(address
				, conf, ugi, typeof(GetUserMappingsProtocolPB), 0);
			return new GetUserMappingsProtocolClientSideTranslatorPB(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		private static HSAdminRefreshProtocol CreateHSProxyWithHSAdminRefreshProtocol(IPEndPoint
			 hsaddr, Configuration conf, UserGroupInformation ugi)
		{
			HSAdminRefreshProtocolPB proxy = (HSAdminRefreshProtocolPB)CreateHSProxy(hsaddr, 
				conf, ugi, typeof(HSAdminRefreshProtocolPB), 0);
			return new HSAdminRefreshProtocolClientSideTranslatorPB(proxy);
		}

		/// <exception cref="System.IO.IOException"/>
		private static object CreateHSProxy(IPEndPoint address, Configuration conf, UserGroupInformation
			 ugi, Type xface, int rpcTimeout)
		{
			RPC.SetProtocolEngine(conf, xface, typeof(ProtobufRpcEngine));
			object proxy = RPC.GetProxy(xface, RPC.GetProtocolVersion(xface), address, ugi, conf
				, NetUtils.GetDefaultSocketFactory(conf), rpcTimeout);
			return proxy;
		}
	}
}
