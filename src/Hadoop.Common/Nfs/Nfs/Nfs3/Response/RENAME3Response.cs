using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>RENAME3 Response</summary>
	public class RENAME3Response : NFS3Response
	{
		private readonly WccData fromDirWcc;

		private readonly WccData toDirWcc;

		public RENAME3Response(int status)
			: this(status, new WccData(null, null), new WccData(null, null))
		{
		}

		public RENAME3Response(int status, WccData fromWccData, WccData toWccData)
			: base(status)
		{
			this.fromDirWcc = fromWccData;
			this.toDirWcc = toWccData;
		}

		public virtual WccData GetFromDirWcc()
		{
			return fromDirWcc;
		}

		public virtual WccData GetToDirWcc()
		{
			return toDirWcc;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.RENAME3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			WccData fromDirWcc = WccData.Deserialize(xdr);
			WccData toDirWcc = WccData.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.RENAME3Response(status, fromDirWcc
				, toDirWcc);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			fromDirWcc.Serialize(@out);
			toDirWcc.Serialize(@out);
			return @out;
		}
	}
}
