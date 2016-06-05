using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>GETATTR3 Response</summary>
	public class GETATTR3Response : NFS3Response
	{
		private Nfs3FileAttributes postOpAttr;

		public GETATTR3Response(int status)
			: this(status, new Nfs3FileAttributes())
		{
		}

		public GETATTR3Response(int status, Nfs3FileAttributes attrs)
			: base(status)
		{
			this.postOpAttr = attrs;
		}

		public virtual void SetPostOpAttr(Nfs3FileAttributes postOpAttr)
		{
			this.postOpAttr = postOpAttr;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.GETATTR3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			Nfs3FileAttributes attr = (status == Nfs3Status.Nfs3Ok) ? Nfs3FileAttributes.Deserialize
				(xdr) : new Nfs3FileAttributes();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.GETATTR3Response(status, attr);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				postOpAttr.Serialize(@out);
			}
			return @out;
		}
	}
}
