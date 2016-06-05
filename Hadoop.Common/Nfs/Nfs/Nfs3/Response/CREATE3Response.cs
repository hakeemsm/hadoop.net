using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>CREATE3 Response</summary>
	public class CREATE3Response : NFS3Response
	{
		private readonly FileHandle objHandle;

		private readonly Nfs3FileAttributes postOpObjAttr;

		private WccData dirWcc;

		public CREATE3Response(int status)
			: this(status, null, null, null)
		{
		}

		public CREATE3Response(int status, FileHandle handle, Nfs3FileAttributes postOpObjAttr
			, WccData dirWcc)
			: base(status)
		{
			this.objHandle = handle;
			this.postOpObjAttr = postOpObjAttr;
			this.dirWcc = dirWcc;
		}

		public virtual FileHandle GetObjHandle()
		{
			return objHandle;
		}

		public virtual Nfs3FileAttributes GetPostOpObjAttr()
		{
			return postOpObjAttr;
		}

		public virtual WccData GetDirWcc()
		{
			return dirWcc;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.CREATE3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			FileHandle objHandle = new FileHandle();
			Nfs3FileAttributes postOpObjAttr = null;
			if (status == Nfs3Status.Nfs3Ok)
			{
				xdr.ReadBoolean();
				objHandle.Deserialize(xdr);
				xdr.ReadBoolean();
				postOpObjAttr = Nfs3FileAttributes.Deserialize(xdr);
			}
			WccData dirWcc = WccData.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.CREATE3Response(status, objHandle, 
				postOpObjAttr, dirWcc);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteBoolean(true);
				// Handle follows
				objHandle.Serialize(@out);
				@out.WriteBoolean(true);
				// Attributes follow
				postOpObjAttr.Serialize(@out);
			}
			if (dirWcc == null)
			{
				dirWcc = new WccData(null, null);
			}
			dirWcc.Serialize(@out);
			return @out;
		}
	}
}
