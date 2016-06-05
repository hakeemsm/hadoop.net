using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>RMDIR3 Response</summary>
	public class RMDIR3Response : NFS3Response
	{
		private readonly WccData dirWcc;

		public RMDIR3Response(int status)
			: this(status, new WccData(null, null))
		{
		}

		public RMDIR3Response(int status, WccData wccData)
			: base(status)
		{
			this.dirWcc = wccData;
		}

		public virtual WccData GetDirWcc()
		{
			return dirWcc;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.RMDIR3Response Deserialize(XDR 
			xdr)
		{
			int status = xdr.ReadInt();
			WccData dirWcc = WccData.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.RMDIR3Response(status, dirWcc);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			dirWcc.Serialize(@out);
			return @out;
		}
	}
}
