using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Security.Client
{
	public class ClientHSSecurityInfo : SecurityInfo
	{
		public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
		{
			if (!protocol.Equals(typeof(HSClientProtocolPB)))
			{
				return null;
			}
			return new _KerberosInfo_40();
		}

		private sealed class _KerberosInfo_40 : KerberosInfo
		{
			public _KerberosInfo_40()
			{
			}

			public Type AnnotationType()
			{
				return null;
			}

			public string ServerPrincipal()
			{
				return JHAdminConfig.MrHistoryPrincipal;
			}

			public string ClientPrincipal()
			{
				return null;
			}
		}

		public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
		{
			if (!protocol.Equals(typeof(HSClientProtocolPB)))
			{
				return null;
			}
			return new _TokenInfo_65();
		}

		private sealed class _TokenInfo_65 : TokenInfo
		{
			public _TokenInfo_65()
			{
			}

			public Type AnnotationType()
			{
				return null;
			}

			public Type Value()
			{
				return typeof(ClientHSTokenSelector);
			}
		}
	}
}
