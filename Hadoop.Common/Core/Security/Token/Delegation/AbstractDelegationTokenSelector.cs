using System.Collections.Generic;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation
{
	/// <summary>
	/// Look through tokens to find the first delegation token that matches the
	/// service and return it.
	/// </summary>
	public class AbstractDelegationTokenSelector<TokenIdent> : TokenSelector<TokenIdent
		>
		where TokenIdent : AbstractDelegationTokenIdentifier
	{
		private Text kindName;

		protected internal AbstractDelegationTokenSelector(Text kindName)
		{
			this.kindName = kindName;
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<TokenIdent> SelectToken(Text
			 service, ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> tokens
			)
		{
			if (service == null)
			{
				return null;
			}
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in tokens)
			{
				if (kindName.Equals(token.GetKind()) && service.Equals(token.GetService()))
				{
					return (Org.Apache.Hadoop.Security.Token.Token<TokenIdent>)token;
				}
			}
			return null;
		}
	}
}
