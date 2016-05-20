using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
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
	/// <see cref="org.apache.hadoop.security.UserGroupInformation.getCurrentUser()"/>
	/// value.
	/// </remarks>
	public class PseudoDelegationTokenAuthenticator : org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticator
	{
		public PseudoDelegationTokenAuthenticator()
			: base(new _PseudoAuthenticator_42())
		{
		}

		private sealed class _PseudoAuthenticator_42 : org.apache.hadoop.security.authentication.client.PseudoAuthenticator
		{
			public _PseudoAuthenticator_42()
			{
			}

			protected internal override string getUserName()
			{
				try
				{
					return org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getShortUserName
						();
				}
				catch (System.IO.IOException ex)
				{
					throw new System.Exception(ex);
				}
			}
		}
	}
}
