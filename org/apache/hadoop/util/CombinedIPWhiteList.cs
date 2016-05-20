using Sharpen;

namespace org.apache.hadoop.util
{
	public class CombinedIPWhiteList : org.apache.hadoop.util.IPList
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.CombinedIPWhiteList
			)));

		private const string LOCALHOST_IP = "127.0.0.1";

		private readonly org.apache.hadoop.util.IPList[] networkLists;

		public CombinedIPWhiteList(string fixedWhiteListFile, string variableWhiteListFile
			, long cacheExpiryInSeconds)
		{
			org.apache.hadoop.util.IPList fixedNetworkList = new org.apache.hadoop.util.FileBasedIPList
				(fixedWhiteListFile);
			if (variableWhiteListFile != null)
			{
				org.apache.hadoop.util.IPList variableNetworkList = new org.apache.hadoop.util.CacheableIPList
					(new org.apache.hadoop.util.FileBasedIPList(variableWhiteListFile), cacheExpiryInSeconds
					);
				networkLists = new org.apache.hadoop.util.IPList[] { fixedNetworkList, variableNetworkList
					 };
			}
			else
			{
				networkLists = new org.apache.hadoop.util.IPList[] { fixedNetworkList };
			}
		}

		public virtual bool isIn(string ipAddress)
		{
			if (ipAddress == null)
			{
				throw new System.ArgumentException("ipAddress is null");
			}
			if (LOCALHOST_IP.Equals(ipAddress))
			{
				return true;
			}
			foreach (org.apache.hadoop.util.IPList networkList in networkLists)
			{
				if (networkList.isIn(ipAddress))
				{
					return true;
				}
			}
			return false;
		}
	}
}
