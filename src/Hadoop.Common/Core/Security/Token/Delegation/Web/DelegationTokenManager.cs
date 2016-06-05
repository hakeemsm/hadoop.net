using System.IO;
using Com.Google.Common.Annotations;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation;


namespace Org.Apache.Hadoop.Security.Token.Delegation.Web
{
	/// <summary>
	/// Delegation Token Manager used by the
	/// <see cref="KerberosDelegationTokenAuthenticationHandler"/>
	/// .
	/// </summary>
	public class DelegationTokenManager
	{
		public const string EnableZkKey = "zk-dt-secret-manager.enable";

		public const string Prefix = "delegation-token.";

		public const string UpdateInterval = Prefix + "update-interval.sec";

		public const long UpdateIntervalDefault = 24 * 60 * 60;

		public const string MaxLifetime = Prefix + "max-lifetime.sec";

		public const long MaxLifetimeDefault = 7 * 24 * 60 * 60;

		public const string RenewInterval = Prefix + "renew-interval.sec";

		public const long RenewIntervalDefault = 24 * 60 * 60;

		public const string RemovalScanInterval = Prefix + "removal-scan-interval.sec";

		public const long RemovalScanIntervalDefault = 60 * 60;

		private class DelegationTokenSecretManager : AbstractDelegationTokenSecretManager
			<DelegationTokenIdentifier>
		{
			private Text tokenKind;

			public DelegationTokenSecretManager(Configuration conf, Text tokenKind)
				: base(conf.GetLong(UpdateInterval, UpdateIntervalDefault) * 1000, conf.GetLong(MaxLifetime
					, MaxLifetimeDefault) * 1000, conf.GetLong(RenewInterval, RenewIntervalDefault) 
					* 1000, conf.GetLong(RemovalScanInterval, RemovalScanIntervalDefault * 1000))
			{
				this.tokenKind = tokenKind;
			}

			public override DelegationTokenIdentifier CreateIdentifier()
			{
				return new DelegationTokenIdentifier(tokenKind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override DelegationTokenIdentifier DecodeTokenIdentifier(Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				return DelegationTokenManager.DecodeToken(token, tokenKind);
			}
		}

		private class ZKSecretManager : ZKDelegationTokenSecretManager<DelegationTokenIdentifier
			>
		{
			private Text tokenKind;

			public ZKSecretManager(Configuration conf, Text tokenKind)
				: base(conf)
			{
				this.tokenKind = tokenKind;
			}

			public override DelegationTokenIdentifier CreateIdentifier()
			{
				return new DelegationTokenIdentifier(tokenKind);
			}

			/// <exception cref="System.IO.IOException"/>
			public override DelegationTokenIdentifier DecodeTokenIdentifier(Org.Apache.Hadoop.Security.Token.Token
				<DelegationTokenIdentifier> token)
			{
				return DelegationTokenManager.DecodeToken(token, tokenKind);
			}
		}

		private AbstractDelegationTokenSecretManager secretManager = null;

		private bool managedSecretManager;

		public DelegationTokenManager(Configuration conf, Text tokenKind)
		{
			if (conf.GetBoolean(EnableZkKey, false))
			{
				this.secretManager = new DelegationTokenManager.ZKSecretManager(conf, tokenKind);
			}
			else
			{
				this.secretManager = new DelegationTokenManager.DelegationTokenSecretManager(conf
					, tokenKind);
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
		public virtual void SetExternalDelegationTokenSecretManager(AbstractDelegationTokenSecretManager
			 secretManager)
		{
			this.secretManager.StopThreads();
			this.secretManager = secretManager;
			managedSecretManager = false;
		}

		public virtual void Init()
		{
			if (managedSecretManager)
			{
				try
				{
					secretManager.StartThreads();
				}
				catch (IOException ex)
				{
					throw new RuntimeException("Could not start " + secretManager.GetType() + ": " + 
						ex.ToString(), ex);
				}
			}
		}

		public virtual void Destroy()
		{
			if (managedSecretManager)
			{
				secretManager.StopThreads();
			}
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
			> CreateToken(UserGroupInformation ugi, string renewer)
		{
			renewer = (renewer == null) ? ugi.GetShortUserName() : renewer;
			string user = ugi.GetUserName();
			Text owner = new Text(user);
			Text realUser = null;
			if (ugi.GetRealUser() != null)
			{
				realUser = new Text(ugi.GetRealUser().GetUserName());
			}
			AbstractDelegationTokenIdentifier tokenIdentifier = (AbstractDelegationTokenIdentifier
				)secretManager.CreateIdentifier();
			tokenIdentifier.SetOwner(owner);
			tokenIdentifier.SetRenewer(new Text(renewer));
			tokenIdentifier.SetRealUser(realUser);
			return new Org.Apache.Hadoop.Security.Token.Token(tokenIdentifier, secretManager);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long RenewToken<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> token
			, string renewer)
			where _T0 : AbstractDelegationTokenIdentifier
		{
			return secretManager.RenewToken(token, renewer);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void CancelToken<_T0>(Org.Apache.Hadoop.Security.Token.Token<_T0> 
			token, string canceler)
			where _T0 : AbstractDelegationTokenIdentifier
		{
			canceler = (canceler != null) ? canceler : VerifyToken(token).GetShortUserName();
			secretManager.CancelToken(token, canceler);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual UserGroupInformation VerifyToken<_T0>(Org.Apache.Hadoop.Security.Token.Token
			<_T0> token)
			where _T0 : AbstractDelegationTokenIdentifier
		{
			AbstractDelegationTokenIdentifier id = secretManager.DecodeTokenIdentifier(token);
			secretManager.VerifyToken(id, token.GetPassword());
			return id.GetUser();
		}

		[VisibleForTesting]
		public virtual AbstractDelegationTokenSecretManager GetDelegationTokenSecretManager
			()
		{
			return secretManager;
		}

		/// <exception cref="System.IO.IOException"/>
		private static DelegationTokenIdentifier DecodeToken(Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> token, Text tokenKind)
		{
			ByteArrayInputStream buf = new ByteArrayInputStream(token.GetIdentifier());
			DataInputStream dis = new DataInputStream(buf);
			DelegationTokenIdentifier id = new DelegationTokenIdentifier(tokenKind);
			id.ReadFields(dis);
			dis.Close();
			return id;
		}
	}
}
