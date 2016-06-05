using System;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Sharedcache;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	/// <summary>An implementation of the SharedCacheClient API.</summary>
	public class SharedCacheClientImpl : SharedCacheClient
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.SharedCacheClientImpl
			));

		private ClientSCMProtocol scmClient;

		private IPEndPoint scmAddress;

		private Configuration conf;

		private SharedCacheChecksum checksum;

		public SharedCacheClientImpl()
			: base(typeof(Org.Apache.Hadoop.Yarn.Client.Api.Impl.SharedCacheClientImpl).FullName
				)
		{
		}

		private static IPEndPoint GetScmAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.ScmClientServerAddress, YarnConfiguration
				.DefaultScmClientServerAddress, YarnConfiguration.DefaultScmClientServerPort);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			if (this.scmAddress == null)
			{
				this.scmAddress = GetScmAddress(conf);
			}
			this.conf = conf;
			this.checksum = SharedCacheChecksumFactory.GetChecksum(conf);
			base.ServiceInit(conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			this.scmClient = CreateClientProxy();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Connecting to Shared Cache Manager at " + this.scmAddress);
			}
			base.ServiceStart();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			StopClientProxy();
			base.ServiceStop();
		}

		[VisibleForTesting]
		protected internal virtual ClientSCMProtocol CreateClientProxy()
		{
			YarnRPC rpc = YarnRPC.Create(GetConfig());
			return (ClientSCMProtocol)rpc.GetProxy(typeof(ClientSCMProtocol), this.scmAddress
				, GetConfig());
		}

		[VisibleForTesting]
		protected internal virtual void StopClientProxy()
		{
			if (this.scmClient != null)
			{
				RPC.StopProxy(this.scmClient);
				this.scmClient = null;
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override Path Use(ApplicationId applicationId, string resourceKey)
		{
			Path resourcePath = null;
			UseSharedCacheResourceRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<UseSharedCacheResourceRequest>();
			request.SetAppId(applicationId);
			request.SetResourceKey(resourceKey);
			try
			{
				UseSharedCacheResourceResponse response = this.scmClient.Use(request);
				if (response != null && response.GetPath() != null)
				{
					resourcePath = new Path(response.GetPath());
				}
			}
			catch (Exception e)
			{
				// Just catching IOException isn't enough.
				// RPC call can throw ConnectionException.
				// We don't handle different exceptions separately at this point.
				throw new YarnException(e);
			}
			return resourcePath;
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		public override void Release(ApplicationId applicationId, string resourceKey)
		{
			ReleaseSharedCacheResourceRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<ReleaseSharedCacheResourceRequest>();
			request.SetAppId(applicationId);
			request.SetResourceKey(resourceKey);
			try
			{
				// We do not care about the response because it is empty.
				this.scmClient.Release(request);
			}
			catch (Exception e)
			{
				// Just catching IOException isn't enough.
				// RPC call can throw ConnectionException.
				throw new YarnException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override string GetFileChecksum(Path sourceFile)
		{
			FileSystem fs = sourceFile.GetFileSystem(this.conf);
			FSDataInputStream @in = null;
			try
			{
				@in = fs.Open(sourceFile);
				return this.checksum.ComputeChecksum(@in);
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
			}
		}
	}
}
