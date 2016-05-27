using Com.Google.Common.Base;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>Credential used by AUTH_NONE</summary>
	public class CredentialsNone : Credentials
	{
		public CredentialsNone()
			: base(RpcAuthInfo.AuthFlavor.AuthNone)
		{
			mCredentialsLength = 0;
		}

		public override void Read(XDR xdr)
		{
			mCredentialsLength = xdr.ReadInt();
			Preconditions.CheckState(mCredentialsLength == 0);
		}

		public override void Write(XDR xdr)
		{
			Preconditions.CheckState(mCredentialsLength == 0);
			xdr.WriteInt(mCredentialsLength);
		}
	}
}
