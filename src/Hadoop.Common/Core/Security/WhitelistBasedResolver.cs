using System.Collections.Generic;
using System.Net;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>An implementation of the SaslPropertiesResolver.</summary>
	/// <remarks>
	/// An implementation of the SaslPropertiesResolver.
	/// Uses a white list of IPs.
	/// If the connection's IP address is in the list of IP addresses, the salProperties
	/// will be unchanged.
	/// If the connection's IP is not in the list of IP addresses, then QOP for the
	/// connection will be restricted to "hadoop.rpc.protection.non-whitelist"
	/// Uses 3 IPList implementations together to form an aggregate whitelist.
	/// 1. ConstantIPList - to check against a set of hardcoded IPs
	/// 2. Fixed IP List - to check against a list of IP addresses which are specified externally, but
	/// will not change over runtime.
	/// 3. Variable IP List - to check against a list of IP addresses which are specified externally and
	/// could change during runtime.
	/// A connection IP address will checked against these 3 IP Lists in the order specified above.
	/// Once a match is found , the IP address is determined to be in whitelist.
	/// The behavior can be configured using a bunch of configuration parameters.
	/// </remarks>
	public class WhitelistBasedResolver : SaslPropertiesResolver
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(WhitelistBasedResolver)
			);

		private const string FixedwhitelistDefaultLocation = "/etc/hadoop/fixedwhitelist";

		private const string VariablewhitelistDefaultLocation = "/etc/hadoop/whitelist";

		/// <summary>Path to the file to containing subnets and ip addresses to form fixed whitelist.
		/// 	</summary>
		public const string HadoopSecuritySaslFixedwhitelistFile = "hadoop.security.sasl.fixedwhitelist.file";

		/// <summary>Enables/Disables variable whitelist</summary>
		public const string HadoopSecuritySaslVariablewhitelistEnable = "hadoop.security.sasl.variablewhitelist.enable";

		/// <summary>Path to the file to containing subnets and ip addresses to form variable whitelist.
		/// 	</summary>
		public const string HadoopSecuritySaslVariablewhitelistFile = "hadoop.security.sasl.variablewhitelist.file";

		/// <summary>time in seconds by which the variable whitelist file is checked for updates
		/// 	</summary>
		public const string HadoopSecuritySaslVariablewhitelistCacheSecs = "hadoop.security.sasl.variablewhitelist.cache.secs";

		/// <summary>
		/// comma separated list containing alternate hadoop.rpc.protection values for
		/// clients which are not in whitelist
		/// </summary>
		public const string HadoopRpcProtectionNonWhitelist = "hadoop.rpc.protection.non-whitelist";

		private CombinedIPWhiteList whiteList;

		private IDictionary<string, string> saslProps;

		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			string fixedFile = conf.Get(HadoopSecuritySaslFixedwhitelistFile, FixedwhitelistDefaultLocation
				);
			string variableFile = null;
			long expiryTime = 0;
			if (conf.GetBoolean(HadoopSecuritySaslVariablewhitelistEnable, false))
			{
				variableFile = conf.Get(HadoopSecuritySaslVariablewhitelistFile, VariablewhitelistDefaultLocation
					);
				expiryTime = conf.GetLong(HadoopSecuritySaslVariablewhitelistCacheSecs, 3600) * 1000;
			}
			whiteList = new CombinedIPWhiteList(fixedFile, variableFile, expiryTime);
			this.saslProps = GetSaslProperties(conf);
		}

		/// <summary>Identify the Sasl Properties to be used for a connection with a client.</summary>
		/// <param name="clientAddress">client's address</param>
		/// <returns>the sasl properties to be used for the connection.</returns>
		public override IDictionary<string, string> GetServerProperties(IPAddress clientAddress
			)
		{
			if (clientAddress == null)
			{
				return saslProps;
			}
			return whiteList.IsIn(clientAddress.GetHostAddress()) ? GetDefaultProperties() : 
				saslProps;
		}

		/// <exception cref="UnknownHostException"/>
		public virtual IDictionary<string, string> GetServerProperties(string clientAddress
			)
		{
			if (clientAddress == null)
			{
				return saslProps;
			}
			return GetServerProperties(Extensions.GetAddressByName(clientAddress));
		}

		internal static IDictionary<string, string> GetSaslProperties(Configuration conf)
		{
			IDictionary<string, string> saslProps = new SortedDictionary<string, string>();
			string[] qop = conf.GetStrings(HadoopRpcProtectionNonWhitelist, SaslRpcServer.QualityOfProtection
				.Privacy.ToString());
			for (int i = 0; i < qop.Length; i++)
			{
				qop[i] = SaslRpcServer.QualityOfProtection.ValueOf(StringUtils.ToUpperCase(qop[i]
					)).GetSaslQop();
			}
			saslProps[Javax.Security.Sasl.Sasl.Qop] = StringUtils.Join(",", qop);
			saslProps[Javax.Security.Sasl.Sasl.ServerAuth] = "true";
			return saslProps;
		}
	}
}
