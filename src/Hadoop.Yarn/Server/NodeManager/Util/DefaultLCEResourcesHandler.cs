using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	public class DefaultLCEResourcesHandler : LCEResourcesHandler
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util.DefaultLCEResourcesHandler
			));

		private Configuration conf;

		public DefaultLCEResourcesHandler()
		{
		}

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		public virtual void Init(LinuxContainerExecutor lce)
		{
		}

		/*
		* LCE Resources Handler interface
		*/
		public virtual void PreExecute(ContainerId containerId, Resource containerResource
			)
		{
		}

		public virtual void PostExecute(ContainerId containerId)
		{
		}

		public virtual string GetResourcesOption(ContainerId containerId)
		{
			return "cgroups=none";
		}
	}
}
