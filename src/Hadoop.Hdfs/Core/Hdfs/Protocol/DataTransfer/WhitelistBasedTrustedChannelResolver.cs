using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer
{
	public class WhitelistBasedTrustedChannelResolver : TrustedChannelResolver
	{
		private CombinedIPWhiteList whiteListForServer;

		private CombinedIPWhiteList whitelistForClient;

		private const string FixedwhitelistDefaultLocation = "/etc/hadoop/fixedwhitelist";

		private const string VariablewhitelistDefaultLocation = "/etc/hadoop/whitelist";

		/// <summary>Path to the file to containing subnets and ip addresses to form fixed whitelist.
		/// 	</summary>
		public const string DfsDatatransferServerFixedwhitelistFile = "dfs.datatransfer.server.fixedwhitelist.file";

		/// <summary>Enables/Disables variable whitelist</summary>
		public const string DfsDatatransferServerVariablewhitelistEnable = "dfs.datatransfer.server.variablewhitelist.enable";

		/// <summary>Path to the file to containing subnets and ip addresses to form variable whitelist.
		/// 	</summary>
		public const string DfsDatatransferServerVariablewhitelistFile = "dfs.datatransfer.server.variablewhitelist.file";

		/// <summary>time in seconds by which the variable whitelist file is checked for updates
		/// 	</summary>
		public const string DfsDatatransferServerVariablewhitelistCacheSecs = "dfs.datatransfer.server.variablewhitelist.cache.secs";

		/// <summary>Path to the file to containing subnets and ip addresses to form fixed whitelist.
		/// 	</summary>
		public const string DfsDatatransferClientFixedwhitelistFile = "dfs.datatransfer.client.fixedwhitelist.file";

		/// <summary>Enables/Disables variable whitelist</summary>
		public const string DfsDatatransferClientVariablewhitelistEnable = "dfs.datatransfer.client.variablewhitelist.enable";

		/// <summary>Path to the file to containing subnets and ip addresses to form variable whitelist.
		/// 	</summary>
		public const string DfsDatatransferClientVariablewhitelistFile = "dfs.datatransfer.client.variablewhitelist.file";

		/// <summary>time in seconds by which the variable whitelist file is checked for updates
		/// 	</summary>
		public const string DfsDatatransferClientVariablewhitelistCacheSecs = "dfs.datatransfer.client.variablewhitelist.cache.secs";

		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			string fixedFile = conf.Get(DfsDatatransferServerFixedwhitelistFile, FixedwhitelistDefaultLocation
				);
			string variableFile = null;
			long expiryTime = 0;
			if (conf.GetBoolean(DfsDatatransferServerVariablewhitelistEnable, false))
			{
				variableFile = conf.Get(DfsDatatransferServerVariablewhitelistFile, VariablewhitelistDefaultLocation
					);
				expiryTime = conf.GetLong(DfsDatatransferServerVariablewhitelistCacheSecs, 3600) 
					* 1000;
			}
			whiteListForServer = new CombinedIPWhiteList(fixedFile, variableFile, expiryTime);
			fixedFile = conf.Get(DfsDatatransferClientFixedwhitelistFile, fixedFile);
			expiryTime = 0;
			if (conf.GetBoolean(DfsDatatransferClientVariablewhitelistEnable, false))
			{
				variableFile = conf.Get(DfsDatatransferClientVariablewhitelistFile, variableFile);
				expiryTime = conf.GetLong(DfsDatatransferClientVariablewhitelistCacheSecs, 3600) 
					* 1000;
			}
			whitelistForClient = new CombinedIPWhiteList(fixedFile, variableFile, expiryTime);
		}

		public override bool IsTrusted()
		{
			try
			{
				return whitelistForClient.IsIn(Sharpen.Runtime.GetLocalHost().GetHostAddress());
			}
			catch (UnknownHostException)
			{
				return false;
			}
		}

		public override bool IsTrusted(IPAddress clientAddress)
		{
			return whiteListForServer.IsIn(clientAddress.GetHostAddress());
		}
	}
}
