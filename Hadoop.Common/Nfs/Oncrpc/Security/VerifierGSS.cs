using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Oncrpc.Security
{
	/// <summary>Verifier mapped to RPCSEC_GSS.</summary>
	public class VerifierGSS : Verifier
	{
		public VerifierGSS()
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
