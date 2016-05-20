using Sharpen;

namespace org.apache.hadoop.security.token.delegation.web
{
	/// <summary>
	/// Delegation Token Manager used by the
	/// <see cref="KerberosDelegationTokenAuthenticationHandler"/>
	/// .
	/// </summary>
	public class DelegationTokenManager
	{
		public const string ENABLE_ZK_KEY = "zk-dt-secret-manager.enable";

		public const string PREFIX = "delegation-token.";

		public const string UPDATE_INTERVAL = PREFIX + "update-interval.sec";

		public const long UPDATE_INTERVAL_DEFAULT = 24 * 60 * 60;

		public const string MAX_LIFETIME = PREFIX + "max-lifetime.sec";

		public const long MAX_LIFETIME_DEFAULT = 7 * 24 * 60 * 60;

		public const string RENEW_INTERVAL = PREFIX + "renew-interval.sec";

		public const long RENEW_INTERVAL_DEFAULT = 24 * 60 * 60;

		public const string REMOVAL_SCAN_INTERVAL = PREFIX + "removal-scan-interval.sec";

		public const long REMOVAL_SCAN_INTERVAL_DEFAULT = 60 * 60;

		private class DelegationTokenSecretManager : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
			<org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier>
		{
			private org.apache.hadoop.io.Text tokenKind;

			public DelegationTokenSecretManager(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.io.Text
				 tokenKind)
				: base(conf.getLong(UPDATE_INTERVAL, UPDATE_INTERVAL_DEFAULT) * 1000, conf.getLong
					(MAX_LIFETIME, MAX_LIFETIME_DEFAULT) * 1000, conf.getLong(RENEW_INTERVAL, RENEW_INTERVAL_DEFAULT
					) * 1000, conf.getLong(REMOVAL_SCAN_INTERVAL, REMOVAL_SCAN_INTERVAL_DEFAULT * 1000
					))
			{
				this.tokenKind = tokenKind;
			}

			public override org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
				 createIdentifier()
			{
				return new org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
					(tokenKind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
				 decodeTokenIdentifier(org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
				> token)
			{
				return org.apache.hadoop.security.token.delegation.web.DelegationTokenManager.decodeToken
					(token, tokenKind);
			}
		}

		private class ZKSecretManager : org.apache.hadoop.security.token.delegation.ZKDelegationTokenSecretManager
			<org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier>
		{
			private org.apache.hadoop.io.Text tokenKind;

			public ZKSecretManager(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.io.Text
				 tokenKind)
				: base(conf)
			{
				this.tokenKind = tokenKind;
			}

			public override org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
				 createIdentifier()
			{
				return new org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
					(tokenKind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
				 decodeTokenIdentifier(org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
				> token)
			{
				return org.apache.hadoop.security.token.delegation.web.DelegationTokenManager.decodeToken
					(token, tokenKind);
			}
		}

		private org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
			 secretManager = null;

		private bool managedSecretManager;

		public DelegationTokenManager(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.io.Text
			 tokenKind)
		{
			if (conf.getBoolean(ENABLE_ZK_KEY, false))
			{
				this.secretManager = new org.apache.hadoop.security.token.delegation.web.DelegationTokenManager.ZKSecretManager
					(conf, tokenKind);
			}
			else
			{
				this.secretManager = new org.apache.hadoop.security.token.delegation.web.DelegationTokenManager.DelegationTokenSecretManager
					(conf, tokenKind);
			}
			managedSecretManager = true;
		}

		/// <summary>
		/// Sets an external <code>DelegationTokenSecretManager</code> instance to
		/// manage creation and verification of Delegation Tokens.
		/// </summary>
		/// <remarks>
		/// Sets an external <code>DelegationTokenSecretManager</code> instance to
		/// manage creation and verification of Delegation Tokens.
		/// <p/>
		/// This is useful for use cases where secrets must be shared across multiple
		/// services.
		/// </remarks>
		/// <param name="secretManager">a <code>DelegationTokenSecretManager</code> instance</param>
		public virtual void setExternalDelegationTokenSecretManager(org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
			 secretManager)
		{
			this.secretManager.stopThreads();
			this.secretManager = secretManager;
			managedSecretManager = false;
		}

		public virtual void init()
		{
			if (managedSecretManager)
			{
				try
				{
					secretManager.startThreads();
				}
				catch (System.IO.IOException ex)
				{
					throw new System.Exception("Could not start " + Sharpen.Runtime.getClassForObject
						(secretManager) + ": " + ex.ToString(), ex);
				}
			}
		}

		public virtual void destroy()
		{
			if (managedSecretManager)
			{
				secretManager.stopThreads();
			}
		}

		public virtual org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
			> createToken(org.apache.hadoop.security.UserGroupInformation ugi, string renewer
			)
		{
			renewer = (renewer == null) ? ugi.getShortUserName() : renewer;
			string user = ugi.getUserName();
			org.apache.hadoop.io.Text owner = new org.apache.hadoop.io.Text(user);
			org.apache.hadoop.io.Text realUser = null;
			if (ugi.getRealUser() != null)
			{
				realUser = new org.apache.hadoop.io.Text(ugi.getRealUser().getUserName());
			}
			org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier tokenIdentifier
				 = (org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
				)secretManager.createIdentifier();
			tokenIdentifier.setOwner(owner);
			tokenIdentifier.setRenewer(new org.apache.hadoop.io.Text(renewer));
			tokenIdentifier.setRealUser(realUser);
			return new org.apache.hadoop.security.token.Token(tokenIdentifier, secretManager);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long renewToken<_T0>(org.apache.hadoop.security.token.Token<_T0> token
			, string renewer)
			where _T0 : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
		{
			return secretManager.renewToken(token, renewer);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void cancelToken<_T0>(org.apache.hadoop.security.token.Token<_T0> 
			token, string canceler)
			where _T0 : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
		{
			canceler = (canceler != null) ? canceler : verifyToken(token).getShortUserName();
			secretManager.cancelToken(token, canceler);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.security.UserGroupInformation verifyToken<_T0>(org.apache.hadoop.security.token.Token
			<_T0> token)
			where _T0 : org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
		{
			org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier id = 
				secretManager.decodeTokenIdentifier(token);
			secretManager.verifyToken(id, token.getPassword());
			return id.getUser();
		}

		[com.google.common.annotations.VisibleForTesting]
		public virtual org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager
			 getDelegationTokenSecretManager()
		{
			return secretManager;
		}

		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
			 decodeToken(org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier
			> token, org.apache.hadoop.io.Text tokenKind)
		{
			java.io.ByteArrayInputStream buf = new java.io.ByteArrayInputStream(token.getIdentifier
				());
			java.io.DataInputStream dis = new java.io.DataInputStream(buf);
			org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier id = new 
				org.apache.hadoop.security.token.delegation.web.DelegationTokenIdentifier(tokenKind
				);
			id.readFields(dis);
			dis.close();
			return id;
		}
	}
}
