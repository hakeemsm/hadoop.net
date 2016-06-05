using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	public class LINK3Response : NFS3Response
	{
		private readonly WccData fromDirWcc;

		private readonly WccData linkDirWcc;

		public LINK3Response(int status)
			: this(status, new WccData(null, null), new WccData(null, null))
		{
		}

		public LINK3Response(int status, WccData fromDirWcc, WccData linkDirWcc)
			: base(status)
		{
			this.fromDirWcc = fromDirWcc;
			this.linkDirWcc = linkDirWcc;
		}

		public virtual WccData GetFromDirWcc()
		{
			return fromDirWcc;
		}

		public virtual WccData GetLinkDirWcc()
		{
			return linkDirWcc;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.LINK3Response Deserialize(XDR xdr
			)
		{
			int status = xdr.ReadInt();
			WccData fromDirWcc = WccData.Deserialize(xdr);
			WccData linkDirWcc = WccData.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.LINK3Response(status, fromDirWcc, 
				linkDirWcc);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			fromDirWcc.Serialize(@out);
			linkDirWcc.Serialize(@out);
			return @out;
		}
	}
}
