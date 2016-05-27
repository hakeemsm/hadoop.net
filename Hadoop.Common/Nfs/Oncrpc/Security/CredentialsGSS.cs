using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>Credential used by RPCSEC_GSS</summary>
	public class CredentialsGSS : Credentials
	{
		public CredentialsGSS()
			: base(RpcAuthInfo.AuthFlavor.RpcsecGss)
		{
		}

		public override void Read(XDR xdr)
		{
		}

		// TODO Auto-generated method stub
		public override void Write(XDR xdr)
		{
		}
		// TODO Auto-generated method stub
	}
}
