using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>REMOVE3 Response</summary>
	public class REMOVE3Response : NFS3Response
	{
		private WccData dirWcc;

		public REMOVE3Response(int status)
			: this(status, null)
		{
		}

		public REMOVE3Response(int status, WccData dirWcc)
			: base(status)
		{
			this.dirWcc = dirWcc;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.REMOVE3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			WccData dirWcc = WccData.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.REMOVE3Response(status, dirWcc);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			if (dirWcc == null)
			{
				dirWcc = new WccData(null, null);
			}
			dirWcc.Serialize(@out);
			return @out;
		}
	}
}
