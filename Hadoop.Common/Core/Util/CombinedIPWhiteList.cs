using System;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	public class CombinedIPWhiteList : IPList
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.CombinedIPWhiteList
			));

		private const string LocalhostIp = "127.0.0.1";

		private readonly IPList[] networkLists;

		public CombinedIPWhiteList(string fixedWhiteListFile, string variableWhiteListFile
			, long cacheExpiryInSeconds)
		{
			IPList fixedNetworkList = new FileBasedIPList(fixedWhiteListFile);
			if (variableWhiteListFile != null)
			{
				IPList variableNetworkList = new CacheableIPList(new FileBasedIPList(variableWhiteListFile
					), cacheExpiryInSeconds);
				networkLists = new IPList[] { fixedNetworkList, variableNetworkList };
			}
			else
			{
				networkLists = new IPList[] { fixedNetworkList };
			}
		}

		public virtual bool IsIn(string ipAddress)
		{
			if (ipAddress == null)
			{
				throw new ArgumentException("ipAddress is null");
			}
			if (LocalhostIp.Equals(ipAddress))
			{
				return true;
			}
			foreach (IPList networkList in networkLists)
			{
				if (networkList.IsIn(ipAddress))
				{
					return true;
				}
			}
			return false;
		}
	}
}
