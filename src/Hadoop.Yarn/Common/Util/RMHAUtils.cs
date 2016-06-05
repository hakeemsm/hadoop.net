using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class RMHAUtils
	{
		public static string FindActiveRMHAId(YarnConfiguration conf)
		{
			YarnConfiguration yarnConf = new YarnConfiguration(conf);
			ICollection<string> rmIds = yarnConf.GetStringCollection(YarnConfiguration.RmHaIds
				);
			foreach (string currentId in rmIds)
			{
				yarnConf.Set(YarnConfiguration.RmHaId, currentId);
				try
				{
					HAServiceProtocol.HAServiceState haState = GetHAState(yarnConf);
					if (haState.Equals(HAServiceProtocol.HAServiceState.Active))
					{
						return currentId;
					}
				}
				catch (Exception)
				{
				}
			}
			// Couldn't check if this RM is active. Do nothing. Worst case,
			// we wouldn't find an Active RM and return null.
			return null;
		}

		// Couldn't find an Active RM
		/// <exception cref="System.Exception"/>
		private static HAServiceProtocol.HAServiceState GetHAState(YarnConfiguration yarnConf
			)
		{
			HAServiceTarget haServiceTarget;
			int rpcTimeoutForChecks = yarnConf.GetInt(CommonConfigurationKeys.HaFcCliCheckTimeoutKey
				, CommonConfigurationKeys.HaFcCliCheckTimeoutDefault);
			yarnConf.Set(CommonConfigurationKeys.HadoopSecurityServiceUserNameKey, yarnConf.Get
				(YarnConfiguration.RmPrincipal, string.Empty));
			haServiceTarget = new RMHAServiceTarget(yarnConf);
			HAServiceProtocol proto = haServiceTarget.GetProxy(yarnConf, rpcTimeoutForChecks);
			HAServiceProtocol.HAServiceState haState = proto.GetServiceStatus().GetState();
			return haState;
		}

		public static IList<string> GetRMHAWebappAddresses(YarnConfiguration conf)
		{
			ICollection<string> rmIds = conf.GetStringCollection(YarnConfiguration.RmHaIds);
			IList<string> addrs = new AList<string>();
			if (YarnConfiguration.UseHttps(conf))
			{
				foreach (string id in rmIds)
				{
					string addr = conf.Get(YarnConfiguration.RmWebappHttpsAddress + "." + id);
					if (addr != null)
					{
						addrs.AddItem(addr);
					}
				}
			}
			else
			{
				foreach (string id in rmIds)
				{
					string addr = conf.Get(YarnConfiguration.RmWebappAddress + "." + id);
					if (addr != null)
					{
						addrs.AddItem(addr);
					}
				}
			}
			return addrs;
		}
	}
}
