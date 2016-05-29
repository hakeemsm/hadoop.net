using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Zookeeper.Data;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	/// <summary>Helper class that provides utility methods specific to ZK operations</summary>
	public class RMZKUtils
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(RMZKUtils));

		/// <summary>Utility method to fetch the ZK ACLs from the configuration</summary>
		/// <exception cref="System.Exception"/>
		public static IList<ACL> GetZKAcls(Configuration conf)
		{
			// Parse authentication from configuration.
			string zkAclConf = conf.Get(YarnConfiguration.RmZkAcl, YarnConfiguration.DefaultRmZkAcl
				);
			try
			{
				zkAclConf = ZKUtil.ResolveConfIndirection(zkAclConf);
				return ZKUtil.ParseACLs(zkAclConf);
			}
			catch (Exception e)
			{
				Log.Error("Couldn't read ACLs based on " + YarnConfiguration.RmZkAcl);
				throw;
			}
		}

		/// <summary>Utility method to fetch ZK auth info from the configuration</summary>
		/// <exception cref="System.Exception"/>
		public static IList<ZKUtil.ZKAuthInfo> GetZKAuths(Configuration conf)
		{
			string zkAuthConf = conf.Get(YarnConfiguration.RmZkAuth);
			try
			{
				zkAuthConf = ZKUtil.ResolveConfIndirection(zkAuthConf);
				if (zkAuthConf != null)
				{
					return ZKUtil.ParseAuth(zkAuthConf);
				}
				else
				{
					return Sharpen.Collections.EmptyList();
				}
			}
			catch (Exception e)
			{
				Log.Error("Couldn't read Auth based on " + YarnConfiguration.RmZkAuth);
				throw;
			}
		}
	}
}
