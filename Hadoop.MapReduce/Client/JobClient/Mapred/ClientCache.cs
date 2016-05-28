using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class ClientCache
	{
		private readonly Configuration conf;

		private readonly ResourceMgrDelegate rm;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.ClientCache
			));

		private IDictionary<JobID, ClientServiceDelegate> cache = new Dictionary<JobID, ClientServiceDelegate
			>();

		private MRClientProtocol hsProxy;

		public ClientCache(Configuration conf, ResourceMgrDelegate rm)
		{
			this.conf = conf;
			this.rm = rm;
		}

		//TODO: evict from the cache on some threshold
		public virtual ClientServiceDelegate GetClient(JobID jobId)
		{
			lock (this)
			{
				if (hsProxy == null)
				{
					try
					{
						hsProxy = InstantiateHistoryProxy();
					}
					catch (IOException e)
					{
						Log.Warn("Could not connect to History server.", e);
						throw new YarnRuntimeException("Could not connect to History server.", e);
					}
				}
				ClientServiceDelegate client = cache[jobId];
				if (client == null)
				{
					client = new ClientServiceDelegate(conf, rm, jobId, hsProxy);
					cache[jobId] = client;
				}
				return client;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual MRClientProtocol GetInitializedHSProxy()
		{
			lock (this)
			{
				if (this.hsProxy == null)
				{
					hsProxy = InstantiateHistoryProxy();
				}
				return this.hsProxy;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual MRClientProtocol InstantiateHistoryProxy()
		{
			string serviceAddr = conf.Get(JHAdminConfig.MrHistoryAddress);
			if (StringUtils.IsEmpty(serviceAddr))
			{
				return null;
			}
			Log.Debug("Connecting to HistoryServer at: " + serviceAddr);
			YarnRPC rpc = YarnRPC.Create(conf);
			Log.Debug("Connected to HistoryServer at: " + serviceAddr);
			UserGroupInformation currentUser = UserGroupInformation.GetCurrentUser();
			return currentUser.DoAs(new _PrivilegedAction_92(this, rpc, serviceAddr));
		}

		private sealed class _PrivilegedAction_92 : PrivilegedAction<MRClientProtocol>
		{
			public _PrivilegedAction_92(ClientCache _enclosing, YarnRPC rpc, string serviceAddr
				)
			{
				this._enclosing = _enclosing;
				this.rpc = rpc;
				this.serviceAddr = serviceAddr;
			}

			public MRClientProtocol Run()
			{
				return (MRClientProtocol)rpc.GetProxy(typeof(HSClientProtocol), NetUtils.CreateSocketAddr
					(serviceAddr), this._enclosing.conf);
			}

			private readonly ClientCache _enclosing;

			private readonly YarnRPC rpc;

			private readonly string serviceAddr;
		}
	}
}
