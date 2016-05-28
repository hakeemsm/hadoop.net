using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class HistoryServerStateStoreServiceFactory
	{
		/// <summary>Constructs an instance of the configured storage class</summary>
		/// <param name="conf">the configuration</param>
		/// <returns>the state storage instance</returns>
		public static HistoryServerStateStoreService GetStore(Configuration conf)
		{
			Type storeClass = typeof(HistoryServerNullStateStoreService);
			bool recoveryEnabled = conf.GetBoolean(JHAdminConfig.MrHsRecoveryEnable, JHAdminConfig
				.DefaultMrHsRecoveryEnable);
			if (recoveryEnabled)
			{
				storeClass = conf.GetClass<HistoryServerStateStoreService>(JHAdminConfig.MrHsStateStore
					, null);
				if (storeClass == null)
				{
					throw new RuntimeException("Unable to locate storage class, check " + JHAdminConfig
						.MrHsStateStore);
				}
			}
			return ReflectionUtils.NewInstance(storeClass, conf);
		}
	}
}
