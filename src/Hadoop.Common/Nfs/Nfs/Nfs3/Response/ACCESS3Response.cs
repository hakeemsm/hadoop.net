using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>ACCESS3 Response</summary>
	public class ACCESS3Response : NFS3Response
	{
		private readonly int access;

		private readonly Nfs3FileAttributes postOpAttr;

		public ACCESS3Response(int status)
			: this(status, new Nfs3FileAttributes(), 0)
		{
		}

		public ACCESS3Response(int status, Nfs3FileAttributes postOpAttr, int access)
			: base(status)
		{
			/*
			* A bit mask of access permissions indicating access rights for the
			* authentication credentials provided with the request.
			*/
			this.postOpAttr = postOpAttr;
			this.access = access;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.ACCESS3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			Nfs3FileAttributes postOpAttr = null;
			int access = 0;
			if (status == Nfs3Status.Nfs3Ok)
			{
				postOpAttr = Nfs3FileAttributes.Deserialize(xdr);
				access = xdr.ReadInt();
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.ACCESS3Response(status, postOpAttr
				, access);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			if (this.GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteBoolean(true);
				postOpAttr.Serialize(@out);
				@out.WriteInt(access);
			}
			else
			{
				@out.WriteBoolean(false);
			}
			return @out;
		}
	}
}
