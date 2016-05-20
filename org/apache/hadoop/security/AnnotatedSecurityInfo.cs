using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>Constructs SecurityInfo from Annotations provided in protocol interface.
	/// 	</summary>
	public class AnnotatedSecurityInfo : org.apache.hadoop.security.SecurityInfo
	{
		public override org.apache.hadoop.security.KerberosInfo getKerberosInfo(java.lang.Class
			 protocol, org.apache.hadoop.conf.Configuration conf)
		{
			return protocol.getAnnotation<org.apache.hadoop.security.KerberosInfo>();
		}

		public override org.apache.hadoop.security.token.TokenInfo getTokenInfo(java.lang.Class
			 protocol, org.apache.hadoop.conf.Configuration conf)
		{
			return protocol.getAnnotation<org.apache.hadoop.security.token.TokenInfo>();
		}
	}
}
