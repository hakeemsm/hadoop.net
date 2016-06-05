using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>SYMLINK3 Response</summary>
	public class SYMLINK3Response : NFS3Response
	{
		private readonly FileHandle objFileHandle;

		private readonly Nfs3FileAttributes objPostOpAttr;

		private readonly WccData dirWcc;

		public SYMLINK3Response(int status)
			: this(status, null, null, new WccData(null, null))
		{
		}

		public SYMLINK3Response(int status, FileHandle handle, Nfs3FileAttributes attrs, 
			WccData dirWcc)
			: base(status)
		{
			this.objFileHandle = handle;
			this.objPostOpAttr = attrs;
			this.dirWcc = dirWcc;
		}

		public virtual FileHandle GetObjFileHandle()
		{
			return objFileHandle;
		}

		public virtual Nfs3FileAttributes GetObjPostOpAttr()
		{
			return objPostOpAttr;
		}

		public virtual WccData GetDirWcc()
		{
			return dirWcc;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.SYMLINK3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			FileHandle objFileHandle = new FileHandle();
			Nfs3FileAttributes objPostOpAttr = null;
			WccData dirWcc;
			if (status == Nfs3Status.Nfs3Ok)
			{
				xdr.ReadBoolean();
				objFileHandle.Deserialize(xdr);
				xdr.ReadBoolean();
				objPostOpAttr = Nfs3FileAttributes.Deserialize(xdr);
			}
			dirWcc = WccData.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.SYMLINK3Response(status, objFileHandle
				, objPostOpAttr, dirWcc);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			if (this.GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteBoolean(true);
				objFileHandle.Serialize(@out);
				@out.WriteBoolean(true);
				objPostOpAttr.Serialize(@out);
			}
			dirWcc.Serialize(@out);
			return @out;
		}
	}
}
