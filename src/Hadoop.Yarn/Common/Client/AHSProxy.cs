using System;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class AHSProxy<T>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(AHSProxy));

		/// <exception cref="System.IO.IOException"/>
		public static T CreateAHSProxy<T>(Configuration conf, IPEndPoint ahsAddress)
		{
			System.Type protocol = typeof(T);
			Log.Info("Connecting to Application History server at " + ahsAddress);
			return (T)GetProxy(conf, protocol, ahsAddress);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal static T GetProxy<T>(Configuration conf, IPEndPoint rmAddress)
		{
			System.Type protocol = typeof(T);
			return UserGroupInformation.GetCurrentUser().DoAs(new _PrivilegedAction_50(conf, 
				protocol, rmAddress));
		}

		private sealed class _PrivilegedAction_50 : PrivilegedAction<T>
		{
			public _PrivilegedAction_50(Configuration conf, Type protocol, IPEndPoint rmAddress
				)
			{
				this.conf = conf;
				this.protocol = protocol;
				this.rmAddress = rmAddress;
			}

			public T Run()
			{
				return (T)YarnRPC.Create(conf).GetProxy(protocol, rmAddress, conf);
			}

			private readonly Configuration conf;

			private readonly Type protocol;

			private readonly IPEndPoint rmAddress;
		}
	}
}
