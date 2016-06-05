using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	public class NMTokenSelector : TokenSelector<NMTokenIdentifier>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(NMTokenSelector));

		public virtual Org.Apache.Hadoop.Security.Token.Token<NMTokenIdentifier> SelectToken
			(Text service, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> tokens)
		{
			if (service == null)
			{
				return null;
			}
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Info("Looking for service: " + service + ". Current token is " + token);
				}
				if (NMTokenIdentifier.Kind.Equals(token.GetKind()) && service.Equals(token.GetService
					()))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<NMTokenIdentifier>)token;
				}
			}
			return null;
		}
	}
}
