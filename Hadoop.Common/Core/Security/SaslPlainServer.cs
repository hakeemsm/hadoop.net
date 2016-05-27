using System;
using System.Collections.Generic;
using Javax.Security.Auth.Callback;
using Javax.Security.Sasl;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	public class SaslPlainServer : SaslServer
	{
		[System.Serializable]
		public class SecurityProvider : Provider
		{
			public SecurityProvider()
				: base("SaslPlainServer", 1.0, "SASL PLAIN Authentication Server")
			{
				this["SaslServerFactory.PLAIN"] = typeof(SaslPlainServer.SaslPlainServerFactory).
					FullName;
			}
		}

		public class SaslPlainServerFactory : SaslServerFactory
		{
			/// <exception cref="Javax.Security.Sasl.SaslException"/>
			public virtual SaslServer CreateSaslServer(string mechanism, string protocol, string
				 serverName, IDictionary<string, object> props, CallbackHandler cbh)
			{
				return "PLAIN".Equals(mechanism) ? new SaslPlainServer(cbh) : null;
			}

			public virtual string[] GetMechanismNames(IDictionary<string, object> props)
			{
				return (props == null) || "false".Equals(props[Javax.Security.Sasl.Sasl.PolicyNoplaintext
					]) ? new string[] { "PLAIN" } : new string[0];
			}
		}

		private CallbackHandler cbh;

		private bool completed;

		private string authz;

		internal SaslPlainServer(CallbackHandler callback)
		{
			this.cbh = callback;
		}

		public virtual string GetMechanismName()
		{
			return "PLAIN";
		}

		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		public virtual byte[] EvaluateResponse(byte[] response)
		{
			if (completed)
			{
				throw new InvalidOperationException("PLAIN authentication has completed");
			}
			if (response == null)
			{
				throw new ArgumentException("Received null response");
			}
			try
			{
				string payload;
				try
				{
					payload = Sharpen.Runtime.GetStringForBytes(response, "UTF-8");
				}
				catch (Exception e)
				{
					throw new ArgumentException("Received corrupt response", e);
				}
				// [ authz, authn, password ]
				string[] parts = payload.Split("\u0000", 3);
				if (parts.Length != 3)
				{
					throw new ArgumentException("Received corrupt response");
				}
				if (parts[0].IsEmpty())
				{
					// authz = authn
					parts[0] = parts[1];
				}
				NameCallback nc = new NameCallback("SASL PLAIN");
				nc.SetName(parts[1]);
				PasswordCallback pc = new PasswordCallback("SASL PLAIN", false);
				pc.SetPassword(parts[2].ToCharArray());
				AuthorizeCallback ac = new AuthorizeCallback(parts[1], parts[0]);
				cbh.Handle(new Javax.Security.Auth.Callback.Callback[] { nc, pc, ac });
				if (ac.IsAuthorized())
				{
					authz = ac.GetAuthorizedID();
				}
			}
			catch (Exception e)
			{
				throw new SaslException("PLAIN auth failed: " + e.Message);
			}
			finally
			{
				completed = true;
			}
			return null;
		}

		private void ThrowIfNotComplete()
		{
			if (!completed)
			{
				throw new InvalidOperationException("PLAIN authentication not completed");
			}
		}

		public virtual bool IsComplete()
		{
			return completed;
		}

		public virtual string GetAuthorizationID()
		{
			ThrowIfNotComplete();
			return authz;
		}

		public virtual object GetNegotiatedProperty(string propName)
		{
			ThrowIfNotComplete();
			return Javax.Security.Sasl.Sasl.Qop.Equals(propName) ? "auth" : null;
		}

		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		public virtual byte[] Wrap(byte[] outgoing, int offset, int len)
		{
			ThrowIfNotComplete();
			throw new InvalidOperationException("PLAIN supports neither integrity nor privacy"
				);
		}

		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		public virtual byte[] Unwrap(byte[] incoming, int offset, int len)
		{
			ThrowIfNotComplete();
			throw new InvalidOperationException("PLAIN supports neither integrity nor privacy"
				);
		}

		/// <exception cref="Javax.Security.Sasl.SaslException"/>
		public virtual void Dispose()
		{
			cbh = null;
			authz = null;
		}
	}
}
