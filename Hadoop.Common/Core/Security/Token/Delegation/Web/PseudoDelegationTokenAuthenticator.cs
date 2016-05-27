using System.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>
	/// The <code>PseudoDelegationTokenAuthenticator</code> provides support for
	/// Hadoop's pseudo authentication mechanism that accepts
	/// the user name specified as a query string parameter and support for Hadoop
	/// Delegation Token operations.
	/// </summary>
	/// <remarks>
	/// The <code>PseudoDelegationTokenAuthenticator</code> provides support for
	/// Hadoop's pseudo authentication mechanism that accepts
	/// the user name specified as a query string parameter and support for Hadoop
	/// Delegation Token operations.
	/// <p/>
	/// This mimics the model of Hadoop Simple authentication trusting the
	/// <see cref="Org.Apache.Hadoop.Security.UserGroupInformation.GetCurrentUser()"/>
	/// value.
	/// </remarks>
	public class PseudoDelegationTokenAuthenticator : DelegationTokenAuthenticator
	{
		public PseudoDelegationTokenAuthenticator()
			: base(new _PseudoAuthenticator_42())
		{
		}

		private sealed class _PseudoAuthenticator_42 : PseudoAuthenticator
		{
			public _PseudoAuthenticator_42()
			{
			}

			protected override string GetUserName()
			{
				try
				{
					return UserGroupInformation.GetCurrentUser().GetShortUserName();
				}
				catch (IOException ex)
				{
					throw new RuntimeException(ex);
				}
			}
		}
	}
}
