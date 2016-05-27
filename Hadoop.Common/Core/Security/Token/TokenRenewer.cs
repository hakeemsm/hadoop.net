using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token
{
	/// <summary>This is the interface for plugins that handle tokens.</summary>
	public abstract class TokenRenewer
	{
		/// <summary>Does this renewer handle this kind of token?</summary>
		/// <param name="kind">the kind of the token</param>
		/// <returns>true if this renewer can renew it</returns>
		public abstract bool HandleKind(Text kind);

		/// <summary>
		/// Is the given token managed? Only managed tokens may be renewed or
		/// cancelled.
		/// </summary>
		/// <param name="token">the token being checked</param>
		/// <returns>true if the token may be renewed or cancelled</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool IsManaged<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			)
			where _T0 : TokenIdentifier;

		/// <summary>Renew the given token.</summary>
		/// <returns>the new expiration time</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"></exception>
		public abstract long Renew<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			, Configuration conf)
			where _T0 : TokenIdentifier;

		/// <summary>Cancel the given token</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"></exception>
		public abstract void Cancel<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			, Configuration conf)
			where _T0 : TokenIdentifier;
	}
}
