using Sharpen;

namespace org.apache.hadoop.security
{
	public class SaslPlainServer : javax.security.sasl.SaslServer
	{
		[System.Serializable]
		public class SecurityProvider : java.security.Provider
		{
			public SecurityProvider()
				: base("SaslPlainServer", 1.0, "SASL PLAIN Authentication Server")
			{
				this["SaslServerFactory.PLAIN"] = Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.SaslPlainServer.SaslPlainServerFactory
					)).getName();
			}
		}

		public class SaslPlainServerFactory : javax.security.sasl.SaslServerFactory
		{
			/// <exception cref="javax.security.sasl.SaslException"/>
			public virtual javax.security.sasl.SaslServer createSaslServer(string mechanism, 
				string protocol, string serverName, System.Collections.Generic.IDictionary<string
				, object> props, javax.security.auth.callback.CallbackHandler cbh)
			{
				return "PLAIN".Equals(mechanism) ? new org.apache.hadoop.security.SaslPlainServer
					(cbh) : null;
			}

			public virtual string[] getMechanismNames(System.Collections.Generic.IDictionary<
				string, object> props)
			{
				return (props == null) || "false".Equals(props[javax.security.sasl.Sasl.POLICY_NOPLAINTEXT
					]) ? new string[] { "PLAIN" } : new string[0];
			}
		}

		private javax.security.auth.callback.CallbackHandler cbh;

		private bool completed;

		private string authz;

		internal SaslPlainServer(javax.security.auth.callback.CallbackHandler callback)
		{
			this.cbh = callback;
		}

		public virtual string getMechanismName()
		{
			return "PLAIN";
		}

		/// <exception cref="javax.security.sasl.SaslException"/>
		public virtual byte[] evaluateResponse(byte[] response)
		{
			if (completed)
			{
				throw new System.InvalidOperationException("PLAIN authentication has completed");
			}
			if (response == null)
			{
				throw new System.ArgumentException("Received null response");
			}
			try
			{
				string payload;
				try
				{
					payload = Sharpen.Runtime.getStringForBytes(response, "UTF-8");
				}
				catch (System.Exception e)
				{
					throw new System.ArgumentException("Received corrupt response", e);
				}
				// [ authz, authn, password ]
				string[] parts = payload.split("\u0000", 3);
				if (parts.Length != 3)
				{
					throw new System.ArgumentException("Received corrupt response");
				}
				if (parts[0].isEmpty())
				{
					// authz = authn
					parts[0] = parts[1];
				}
				javax.security.auth.callback.NameCallback nc = new javax.security.auth.callback.NameCallback
					("SASL PLAIN");
				nc.setName(parts[1]);
				javax.security.auth.callback.PasswordCallback pc = new javax.security.auth.callback.PasswordCallback
					("SASL PLAIN", false);
				pc.setPassword(parts[2].ToCharArray());
				javax.security.sasl.AuthorizeCallback ac = new javax.security.sasl.AuthorizeCallback
					(parts[1], parts[0]);
				cbh.handle(new javax.security.auth.callback.Callback[] { nc, pc, ac });
				if (ac.isAuthorized())
				{
					authz = ac.getAuthorizedID();
				}
			}
			catch (System.Exception e)
			{
				throw new javax.security.sasl.SaslException("PLAIN auth failed: " + e.Message);
			}
			finally
			{
				completed = true;
			}
			return null;
		}

		private void throwIfNotComplete()
		{
			if (!completed)
			{
				throw new System.InvalidOperationException("PLAIN authentication not completed");
			}
		}

		public virtual bool isComplete()
		{
			return completed;
		}

		public virtual string getAuthorizationID()
		{
			throwIfNotComplete();
			return authz;
		}

		public virtual object getNegotiatedProperty(string propName)
		{
			throwIfNotComplete();
			return javax.security.sasl.Sasl.QOP.Equals(propName) ? "auth" : null;
		}

		/// <exception cref="javax.security.sasl.SaslException"/>
		public virtual byte[] wrap(byte[] outgoing, int offset, int len)
		{
			throwIfNotComplete();
			throw new System.InvalidOperationException("PLAIN supports neither integrity nor privacy"
				);
		}

		/// <exception cref="javax.security.sasl.SaslException"/>
		public virtual byte[] unwrap(byte[] incoming, int offset, int len)
		{
			throwIfNotComplete();
			throw new System.InvalidOperationException("PLAIN supports neither integrity nor privacy"
				);
		}

		/// <exception cref="javax.security.sasl.SaslException"/>
		public virtual void dispose()
		{
			cbh = null;
			authz = null;
		}
	}
}
