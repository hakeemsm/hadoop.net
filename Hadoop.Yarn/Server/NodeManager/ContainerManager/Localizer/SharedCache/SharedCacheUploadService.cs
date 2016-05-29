using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache
{
	public class SharedCacheUploadService : AbstractService, EventHandler<SharedCacheUploadEvent
		>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache.SharedCacheUploadService
			));

		private bool enabled;

		private FileSystem fs;

		private FileSystem localFs;

		private ExecutorService uploaderPool;

		private SCMUploaderProtocol scmClient;

		public SharedCacheUploadService()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Sharedcache.SharedCacheUploadService
				).FullName)
		{
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			enabled = conf.GetBoolean(YarnConfiguration.SharedCacheEnabled, YarnConfiguration
				.DefaultSharedCacheEnabled);
			if (enabled)
			{
				int threadCount = conf.GetInt(YarnConfiguration.SharedCacheNmUploaderThreadCount, 
					YarnConfiguration.DefaultSharedCacheNmUploaderThreadCount);
				uploaderPool = Executors.NewFixedThreadPool(threadCount, new ThreadFactoryBuilder
					().SetNameFormat("Shared cache uploader #%d").Build());
				scmClient = CreateSCMClient(conf);
				try
				{
					fs = FileSystem.Get(conf);
					localFs = FileSystem.GetLocal(conf);
				}
				catch (IOException e)
				{
					Log.Error("Unexpected exception in getting the filesystem", e);
					throw new RuntimeException(e);
				}
			}
			base.ServiceInit(conf);
		}

		private SCMUploaderProtocol CreateSCMClient(Configuration conf)
		{
			YarnRPC rpc = YarnRPC.Create(conf);
			IPEndPoint scmAddress = conf.GetSocketAddr(YarnConfiguration.ScmUploaderServerAddress
				, YarnConfiguration.DefaultScmUploaderServerAddress, YarnConfiguration.DefaultScmUploaderServerPort
				);
			return (SCMUploaderProtocol)rpc.GetProxy(typeof(SCMUploaderProtocol), scmAddress, 
				conf);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (enabled)
			{
				uploaderPool.Shutdown();
				RPC.StopProxy(scmClient);
			}
			base.ServiceStop();
		}

		public virtual void Handle(SharedCacheUploadEvent @event)
		{
			if (enabled)
			{
				IDictionary<LocalResourceRequest, Path> resources = @event.GetResources();
				foreach (KeyValuePair<LocalResourceRequest, Path> e in resources)
				{
					SharedCacheUploader uploader = new SharedCacheUploader(e.Key, e.Value, @event.GetUser
						(), GetConfig(), scmClient, fs, localFs);
					// fire off an upload task
					uploaderPool.Submit(uploader);
				}
			}
		}

		public virtual bool IsEnabled()
		{
			return enabled;
		}
	}
}
