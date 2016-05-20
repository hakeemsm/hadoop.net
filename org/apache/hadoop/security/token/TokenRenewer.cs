using Sharpen;

namespace org.apache.hadoop.security.token
{
	/// <summary>This is the interface for plugins that handle tokens.</summary>
	public abstract class TokenRenewer
	{
		/// <summary>Does this renewer handle this kind of token?</summary>
		/// <param name="kind">the kind of the token</param>
		/// <returns>true if this renewer can renew it</returns>
		public abstract bool handleKind(org.apache.hadoop.io.Text kind);

		/// <summary>
		/// Is the given token managed? Only managed tokens may be renewed or
		/// cancelled.
		/// </summary>
		/// <param name="token">the token being checked</param>
		/// <returns>true if the token may be renewed or cancelled</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool isManaged<_T0>(org.apache.hadoop.security.token.Token<_T0> token
			)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier;

		/// <summary>Renew the given token.</summary>
		/// <returns>the new expiration time</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"></exception>
		public abstract long renew<_T0>(org.apache.hadoop.security.token.Token<_T0> token
			, org.apache.hadoop.conf.Configuration conf)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier;

		/// <summary>Cancel the given token</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"></exception>
		public abstract void cancel<_T0>(org.apache.hadoop.security.token.Token<_T0> token
			, org.apache.hadoop.conf.Configuration conf)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier;
	}
}
