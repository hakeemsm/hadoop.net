using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>Provides SaslProperties to be used for a connection.</summary>
	/// <remarks>
	/// Provides SaslProperties to be used for a connection.
	/// The default implementation is to read the values from configuration.
	/// This class can be overridden to provide custom SaslProperties.
	/// The custom class can be specified via configuration.
	/// </remarks>
	public class SaslPropertiesResolver : org.apache.hadoop.conf.Configurable
	{
		private System.Collections.Generic.IDictionary<string, string> properties;

		internal org.apache.hadoop.conf.Configuration conf;

		/// <summary>Returns an instance of SaslPropertiesResolver.</summary>
		/// <remarks>
		/// Returns an instance of SaslPropertiesResolver.
		/// Looks up the configuration to see if there is custom class specified.
		/// Constructs the instance by passing the configuration directly to the
		/// constructor to achieve thread safety using final fields.
		/// </remarks>
		/// <param name="conf"/>
		/// <returns>SaslPropertiesResolver</returns>
		public static org.apache.hadoop.security.SaslPropertiesResolver getInstance(org.apache.hadoop.conf.Configuration
			 conf)
		{
			java.lang.Class clazz = conf.getClass<org.apache.hadoop.security.SaslPropertiesResolver
				>(org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_SASL_PROPS_RESOLVER_CLASS
				, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.SaslPropertiesResolver
				)));
			return org.apache.hadoop.util.ReflectionUtils.newInstance(clazz, conf);
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
			properties = new System.Collections.Generic.SortedDictionary<string, string>();
			string[] qop = conf.getTrimmedStrings(org.apache.hadoop.fs.CommonConfigurationKeysPublic
				.HADOOP_RPC_PROTECTION, org.apache.hadoop.security.SaslRpcServer.QualityOfProtection
				.AUTHENTICATION.ToString());
			for (int i = 0; i < qop.Length; i++)
			{
				qop[i] = org.apache.hadoop.security.SaslRpcServer.QualityOfProtection.valueOf(org.apache.hadoop.util.StringUtils
					.toUpperCase(qop[i])).getSaslQop();
			}
			properties[javax.security.sasl.Sasl.QOP] = org.apache.hadoop.util.StringUtils.join
				(",", qop);
			properties[javax.security.sasl.Sasl.SERVER_AUTH] = "true";
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <summary>The default Sasl Properties read from the configuration</summary>
		/// <returns>sasl Properties</returns>
		public virtual System.Collections.Generic.IDictionary<string, string> getDefaultProperties
			()
		{
			return properties;
		}

		/// <summary>Identify the Sasl Properties to be used for a connection with a  client.
		/// 	</summary>
		/// <param name="clientAddress">client's address</param>
		/// <returns>the sasl properties to be used for the connection.</returns>
		public virtual System.Collections.Generic.IDictionary<string, string> getServerProperties
			(java.net.InetAddress clientAddress)
		{
			return properties;
		}

		/// <summary>Identify the Sasl Properties to be used for a connection with a server.</summary>
		/// <param name="serverAddress">server's address</param>
		/// <returns>the sasl properties to be used for the connection.</returns>
		public virtual System.Collections.Generic.IDictionary<string, string> getClientProperties
			(java.net.InetAddress serverAddress)
		{
			return properties;
		}
	}
}
