using Sharpen;

namespace org.apache.hadoop.security
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
	public class WhitelistBasedResolver : org.apache.hadoop.security.SaslPropertiesResolver
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.WhitelistBasedResolver
			)));

		private const string FIXEDWHITELIST_DEFAULT_LOCATION = "/etc/hadoop/fixedwhitelist";

		private const string VARIABLEWHITELIST_DEFAULT_LOCATION = "/etc/hadoop/whitelist";

		/// <summary>Path to the file to containing subnets and ip addresses to form fixed whitelist.
		/// 	</summary>
		public const string HADOOP_SECURITY_SASL_FIXEDWHITELIST_FILE = "hadoop.security.sasl.fixedwhitelist.file";

		/// <summary>Enables/Disables variable whitelist</summary>
		public const string HADOOP_SECURITY_SASL_VARIABLEWHITELIST_ENABLE = "hadoop.security.sasl.variablewhitelist.enable";

		/// <summary>Path to the file to containing subnets and ip addresses to form variable whitelist.
		/// 	</summary>
		public const string HADOOP_SECURITY_SASL_VARIABLEWHITELIST_FILE = "hadoop.security.sasl.variablewhitelist.file";

		/// <summary>time in seconds by which the variable whitelist file is checked for updates
		/// 	</summary>
		public const string HADOOP_SECURITY_SASL_VARIABLEWHITELIST_CACHE_SECS = "hadoop.security.sasl.variablewhitelist.cache.secs";

		/// <summary>
		/// comma separated list containing alternate hadoop.rpc.protection values for
		/// clients which are not in whitelist
		/// </summary>
		public const string HADOOP_RPC_PROTECTION_NON_WHITELIST = "hadoop.rpc.protection.non-whitelist";

		private org.apache.hadoop.util.CombinedIPWhiteList whiteList;

		private System.Collections.Generic.IDictionary<string, string> saslProps;

		public override void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			base.setConf(conf);
			string fixedFile = conf.get(HADOOP_SECURITY_SASL_FIXEDWHITELIST_FILE, FIXEDWHITELIST_DEFAULT_LOCATION
				);
			string variableFile = null;
			long expiryTime = 0;
			if (conf.getBoolean(HADOOP_SECURITY_SASL_VARIABLEWHITELIST_ENABLE, false))
			{
				variableFile = conf.get(HADOOP_SECURITY_SASL_VARIABLEWHITELIST_FILE, VARIABLEWHITELIST_DEFAULT_LOCATION
					);
				expiryTime = conf.getLong(HADOOP_SECURITY_SASL_VARIABLEWHITELIST_CACHE_SECS, 3600
					) * 1000;
			}
			whiteList = new org.apache.hadoop.util.CombinedIPWhiteList(fixedFile, variableFile
				, expiryTime);
			this.saslProps = getSaslProperties(conf);
		}

		/// <summary>Identify the Sasl Properties to be used for a connection with a client.</summary>
		/// <param name="clientAddress">client's address</param>
		/// <returns>the sasl properties to be used for the connection.</returns>
		public override System.Collections.Generic.IDictionary<string, string> getServerProperties
			(java.net.InetAddress clientAddress)
		{
			if (clientAddress == null)
			{
				return saslProps;
			}
			return whiteList.isIn(clientAddress.getHostAddress()) ? getDefaultProperties() : 
				saslProps;
		}

		/// <exception cref="java.net.UnknownHostException"/>
		public virtual System.Collections.Generic.IDictionary<string, string> getServerProperties
			(string clientAddress)
		{
			if (clientAddress == null)
			{
				return saslProps;
			}
			return getServerProperties(java.net.InetAddress.getByName(clientAddress));
		}

		internal static System.Collections.Generic.IDictionary<string, string> getSaslProperties
			(org.apache.hadoop.conf.Configuration conf)
		{
			System.Collections.Generic.IDictionary<string, string> saslProps = new System.Collections.Generic.SortedDictionary
				<string, string>();
			string[] qop = conf.getStrings(HADOOP_RPC_PROTECTION_NON_WHITELIST, org.apache.hadoop.security.SaslRpcServer.QualityOfProtection
				.PRIVACY.ToString());
			for (int i = 0; i < qop.Length; i++)
			{
				qop[i] = org.apache.hadoop.security.SaslRpcServer.QualityOfProtection.valueOf(org.apache.hadoop.util.StringUtils
					.toUpperCase(qop[i])).getSaslQop();
			}
			saslProps[javax.security.sasl.Sasl.QOP] = org.apache.hadoop.util.StringUtils.join
				(",", qop);
			saslProps[javax.security.sasl.Sasl.SERVER_AUTH] = "true";
			return saslProps;
		}
	}
}
