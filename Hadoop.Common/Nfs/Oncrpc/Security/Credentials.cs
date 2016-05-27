using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>Base class for all credentials.</summary>
	/// <remarks>
	/// Base class for all credentials. Currently we only support 3 different types
	/// of auth flavors: AUTH_NONE, AUTH_SYS, and RPCSEC_GSS.
	/// </remarks>
	public abstract class Credentials : RpcAuthInfo
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Oncrpc.Security.Credentials
			));

		public static Org.Apache.Hadoop.Oncrpc.Security.Credentials ReadFlavorAndCredentials
			(XDR xdr)
		{
			RpcAuthInfo.AuthFlavor flavor = RpcAuthInfo.AuthFlavor.FromValue(xdr.ReadInt());
			Org.Apache.Hadoop.Oncrpc.Security.Credentials credentials;
			if (flavor == RpcAuthInfo.AuthFlavor.AuthNone)
			{
				credentials = new CredentialsNone();
			}
			else
			{
				if (flavor == RpcAuthInfo.AuthFlavor.AuthSys)
				{
					credentials = new CredentialsSys();
				}
				else
				{
					if (flavor == RpcAuthInfo.AuthFlavor.RpcsecGss)
					{
						credentials = new CredentialsGSS();
					}
					else
					{
						throw new NotSupportedException("Unsupported Credentials Flavor " + flavor);
					}
				}
			}
			credentials.Read(xdr);
			return credentials;
		}

		/// <summary>Write AuthFlavor and the credentials to the XDR</summary>
		public static void WriteFlavorAndCredentials(Org.Apache.Hadoop.Oncrpc.Security.Credentials
			 cred, XDR xdr)
		{
			if (cred is CredentialsNone)
			{
				xdr.WriteInt(RpcAuthInfo.AuthFlavor.AuthNone.GetValue());
			}
			else
			{
				if (cred is CredentialsSys)
				{
					xdr.WriteInt(RpcAuthInfo.AuthFlavor.AuthSys.GetValue());
				}
				else
				{
					if (cred is CredentialsGSS)
					{
						xdr.WriteInt(RpcAuthInfo.AuthFlavor.RpcsecGss.GetValue());
					}
					else
					{
						throw new NotSupportedException("Cannot recognize the verifier");
					}
				}
			}
			cred.Write(xdr);
		}

		protected internal int mCredentialsLength;

		protected internal Credentials(RpcAuthInfo.AuthFlavor flavor)
			: base(flavor)
		{
		}
	}
}
