using System;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>Base class for verifier.</summary>
	/// <remarks>
	/// Base class for verifier. Currently our authentication only supports 3 types
	/// of auth flavors:
	/// <see cref="AuthFlavor.AuthNone"/>
	/// ,
	/// <see cref="AuthFlavor.AuthSys"/>
	/// ,
	/// and
	/// <see cref="AuthFlavor.RpcsecGss"/>
	/// . Thus for verifier we only need to handle
	/// AUTH_NONE and RPCSEC_GSS
	/// </remarks>
	public abstract class Verifier : RpcAuthInfo
	{
		public static readonly Org.Apache.Hadoop.Oncrpc.Security.Verifier VerifierNone = 
			new VerifierNone();

		protected internal Verifier(RpcAuthInfo.AuthFlavor flavor)
			: base(flavor)
		{
		}

		/// <summary>Read both AuthFlavor and the verifier from the XDR</summary>
		public static Org.Apache.Hadoop.Oncrpc.Security.Verifier ReadFlavorAndVerifier(XDR
			 xdr)
		{
			RpcAuthInfo.AuthFlavor flavor = RpcAuthInfo.AuthFlavor.FromValue(xdr.ReadInt());
			Org.Apache.Hadoop.Oncrpc.Security.Verifier verifer;
			if (flavor == RpcAuthInfo.AuthFlavor.AuthNone)
			{
				verifer = new VerifierNone();
			}
			else
			{
				if (flavor == RpcAuthInfo.AuthFlavor.RpcsecGss)
				{
					verifer = new VerifierGSS();
				}
				else
				{
					throw new NotSupportedException("Unsupported verifier flavor" + flavor);
				}
			}
			verifer.Read(xdr);
			return verifer;
		}

		/// <summary>Write AuthFlavor and the verifier to the XDR</summary>
		public static void WriteFlavorAndVerifier(Org.Apache.Hadoop.Oncrpc.Security.Verifier
			 verifier, XDR xdr)
		{
			if (verifier is VerifierNone)
			{
				xdr.WriteInt(RpcAuthInfo.AuthFlavor.AuthNone.GetValue());
			}
			else
			{
				if (verifier is VerifierGSS)
				{
					xdr.WriteInt(RpcAuthInfo.AuthFlavor.RpcsecGss.GetValue());
				}
				else
				{
					throw new NotSupportedException("Cannot recognize the verifier");
				}
			}
			verifier.Write(xdr);
		}
	}
}
