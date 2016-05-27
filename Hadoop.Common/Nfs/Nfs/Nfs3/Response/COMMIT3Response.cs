using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>COMMIT3 Response</summary>
	public class COMMIT3Response : NFS3Response
	{
		private readonly WccData fileWcc;

		private readonly long verf;

		public COMMIT3Response(int status)
			: this(status, new WccData(null, null), Nfs3Constant.WriteCommitVerf)
		{
		}

		public COMMIT3Response(int status, WccData fileWcc, long verf)
			: base(status)
		{
			this.fileWcc = fileWcc;
			this.verf = verf;
		}

		public virtual WccData GetFileWcc()
		{
			return fileWcc;
		}

		public virtual long GetVerf()
		{
			return verf;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.COMMIT3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			long verf = 0;
			WccData fileWcc = WccData.Deserialize(xdr);
			if (status == Nfs3Status.Nfs3Ok)
			{
				verf = xdr.ReadHyper();
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.COMMIT3Response(status, fileWcc, verf
				);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			fileWcc.Serialize(@out);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteLongAsHyper(verf);
			}
			return @out;
		}
	}
}
