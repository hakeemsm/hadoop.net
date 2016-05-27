using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>SETATTR3 Response</summary>
	public class SETATTR3Response : NFS3Response
	{
		private readonly WccData wccData;

		public SETATTR3Response(int status)
			: this(status, new WccData(null, null))
		{
		}

		public SETATTR3Response(int status, WccData wccData)
			: base(status)
		{
			this.wccData = wccData;
		}

		public virtual WccData GetWccData()
		{
			return wccData;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.SETATTR3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			WccData wccData = WccData.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.SETATTR3Response(status, wccData);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			wccData.Serialize(@out);
			return @out;
		}
	}
}
