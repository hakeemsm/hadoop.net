using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	public class AMRMTokenSelector : TokenSelector<AMRMTokenIdentifier>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(AMRMTokenSelector));

		public virtual Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> SelectToken
			(Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> tokens)
		{
			if (service == null)
			{
				return null;
			}
			Log.Debug("Looking for a token with service " + service.ToString());
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
			{
				Log.Debug("Token kind is " + token.GetKind().ToString() + " and the token's service name is "
					 + token.GetService());
				if (AMRMTokenIdentifier.KindName.Equals(token.GetKind()) && CheckService(service, 
					token))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier>)token;
				}
			}
			return null;
		}

		private bool CheckService<_T0>(Text service, Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : TokenIdentifier
		{
			if (service == null || token.GetService() == null)
			{
				return false;
			}
			return token.GetService().ToString().Contains(service.ToString());
		}
	}
}
