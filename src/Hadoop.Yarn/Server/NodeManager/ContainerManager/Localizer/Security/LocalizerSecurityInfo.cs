using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Security
{
	public class LocalizerSecurityInfo : SecurityInfo
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(LocalizerSecurityInfo)
			);

		public override KerberosInfo GetKerberosInfo(Type protocol, Configuration conf)
		{
			return null;
		}

		public override TokenInfo GetTokenInfo(Type protocol, Configuration conf)
		{
			if (!protocol.Equals(typeof(LocalizationProtocolPB)))
			{
				return null;
			}
			return new _TokenInfo_48();
		}

		private sealed class _TokenInfo_48 : TokenInfo
		{
			public _TokenInfo_48()
			{
			}

			public Type AnnotationType()
			{
				return null;
			}

			public Type Value()
			{
				LocalizerSecurityInfo.Log.Debug("Using localizerTokenSecurityInfo");
				return typeof(LocalizerTokenSelector);
			}
		}
	}
}
