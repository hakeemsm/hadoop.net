using Com.Google.Common.Base;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>Verifier used by AUTH_NONE.</summary>
	public class VerifierNone : Verifier
	{
		public VerifierNone()
			: base(RpcAuthInfo.AuthFlavor.AuthNone)
		{
		}

		public override void Read(XDR xdr)
		{
			int length = xdr.ReadInt();
			Preconditions.CheckState(length == 0);
		}

		public override void Write(XDR xdr)
		{
			xdr.WriteInt(0);
		}
	}
}
