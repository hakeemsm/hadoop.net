using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Security
{
	public class ContainerTokenSelector : TokenSelector<ContainerTokenIdentifier>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ContainerTokenSelector
			));

		public virtual Org.Apache.Hadoop.Security.Token.Token<ContainerTokenIdentifier> SelectToken
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
				if (ContainerTokenIdentifier.Kind.Equals(token.GetKind()) && service.Equals(token
					.GetService()))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<ContainerTokenIdentifier>)token;
				}
			}
			return null;
		}
	}
}
