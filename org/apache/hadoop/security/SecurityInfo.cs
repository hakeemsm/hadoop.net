using Sharpen;

namespace org.apache.hadoop.security
{
	public abstract class SecurityInfo
	{
		/// <summary>Get the KerberosInfo for a given protocol.</summary>
		/// <param name="protocol">interface class</param>
		/// <param name="conf">configuration</param>
		/// <returns>KerberosInfo</returns>
		public abstract org.apache.hadoop.security.KerberosInfo getKerberosInfo(java.lang.Class
			 protocol, org.apache.hadoop.conf.Configuration conf);

		/// <summary>Get the TokenInfo for a given protocol.</summary>
		/// <param name="protocol">interface class</param>
		/// <param name="conf">configuration object.</param>
		/// <returns>TokenInfo instance</returns>
		public abstract org.apache.hadoop.security.token.TokenInfo getTokenInfo(java.lang.Class
			 protocol, org.apache.hadoop.conf.Configuration conf);
	}
}
