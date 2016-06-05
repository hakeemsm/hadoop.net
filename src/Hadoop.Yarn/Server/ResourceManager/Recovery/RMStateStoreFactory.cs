using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class RMStateStoreFactory
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(RMStateStoreFactory));

		public static RMStateStore GetStore(Configuration conf)
		{
			Type storeClass = conf.GetClass<RMStateStore>(YarnConfiguration.RmStore, typeof(MemoryRMStateStore
				));
			Log.Info("Using RMStateStore implementation - " + storeClass);
			return ReflectionUtils.NewInstance(storeClass, conf);
		}
	}
}
