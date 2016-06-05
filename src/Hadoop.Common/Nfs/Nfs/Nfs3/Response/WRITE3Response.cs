using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>WRITE3 Response</summary>
	public class WRITE3Response : NFS3Response
	{
		private readonly WccData fileWcc;

		private readonly int count;

		private readonly Nfs3Constant.WriteStableHow stableHow;

		private readonly long verifer;

		public WRITE3Response(int status)
			: this(status, new WccData(null, null), 0, Nfs3Constant.WriteStableHow.Unstable, 
				Nfs3Constant.WriteCommitVerf)
		{
		}

		public WRITE3Response(int status, WccData fileWcc, int count, Nfs3Constant.WriteStableHow
			 stableHow, long verifier)
			: base(status)
		{
			// return on both success and failure
			this.fileWcc = fileWcc;
			this.count = count;
			this.stableHow = stableHow;
			this.verifer = verifier;
		}

		public virtual int GetCount()
		{
			return count;
		}

		public virtual Nfs3Constant.WriteStableHow GetStableHow()
		{
			return stableHow;
		}

		public virtual long GetVerifer()
		{
			return verifer;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.WRITE3Response Deserialize(XDR 
			xdr)
		{
			int status = xdr.ReadInt();
			WccData fileWcc = WccData.Deserialize(xdr);
			int count = 0;
			Nfs3Constant.WriteStableHow stableHow = null;
			long verifier = 0;
			if (status == Nfs3Status.Nfs3Ok)
			{
				count = xdr.ReadInt();
				int how = xdr.ReadInt();
				stableHow = Nfs3Constant.WriteStableHow.Values()[how];
				verifier = xdr.ReadHyper();
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.WRITE3Response(status, fileWcc, count
				, stableHow, verifier);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			fileWcc.Serialize(@out);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteInt(count);
				@out.WriteInt(stableHow.GetValue());
				@out.WriteLongAsHyper(verifer);
			}
			return @out;
		}
	}
}
