using Sharpen;

namespace org.apache.hadoop.security.token.delegation
{
	/// <summary>
	/// Look through tokens to find the first delegation token that matches the
	/// service and return it.
	/// </summary>
	public class AbstractDelegationTokenSelector<TokenIdent> : org.apache.hadoop.security.token.TokenSelector
		<TokenIdent>
		where TokenIdent : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
	{
		private org.apache.hadoop.io.Text kindName;

		protected internal AbstractDelegationTokenSelector(org.apache.hadoop.io.Text kindName
			)
		{
			this.kindName = kindName;
		}

		public virtual org.apache.hadoop.security.token.Token<TokenIdent> selectToken(org.apache.hadoop.io.Text
			 service, System.Collections.Generic.ICollection<org.apache.hadoop.security.token.Token
			<org.apache.hadoop.security.token.TokenIdentifier>> tokens)
		{
			if (service == null)
			{
				return null;
			}
			foreach (org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.TokenIdentifier
				> token in tokens)
			{
				if (kindName.Equals(token.getKind()) && service.Equals(token.getService()))
				{
					return (org.apache.hadoop.security.token.Token<TokenIdent>)token;
				}
			}
			return null;
		}
	}
}
