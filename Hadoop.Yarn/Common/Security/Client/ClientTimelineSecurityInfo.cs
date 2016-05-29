using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security.Client
{
	public class ClientTimelineSecurityInfo : SecurityInfo
	{
		public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
		{
			if (!protocol.Equals(typeof(ApplicationHistoryProtocolPB)))
			{
				return null;
			}
			return new _KerberosInfo_44();
		}

		private sealed class _KerberosInfo_44 : KerberosInfo
		{
			public _KerberosInfo_44()
			{
			}

			public Type AnnotationType()
			{
				return null;
			}

			public string ServerPrincipal()
			{
				return YarnConfiguration.TimelineServicePrincipal;
			}

			public string ClientPrincipal()
			{
				return null;
			}
		}

		public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
		{
			if (!protocol.Equals(typeof(ApplicationHistoryProtocolPB)))
			{
				return null;
			}
			return new _TokenInfo_69();
		}

		private sealed class _TokenInfo_69 : TokenInfo
		{
			public _TokenInfo_69()
			{
			}

			public Type AnnotationType()
			{
				return null;
			}

			public Type Value()
			{
				return typeof(TimelineDelegationTokenSelector);
			}
		}
	}
}
