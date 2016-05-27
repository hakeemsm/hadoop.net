using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>MKDIR3 Response</summary>
	public class MKDIR3Response : NFS3Response
	{
		private readonly FileHandle objFileHandle;

		private readonly Nfs3FileAttributes objAttr;

		private readonly WccData dirWcc;

		public MKDIR3Response(int status)
			: this(status, null, null, new WccData(null, null))
		{
		}

		public MKDIR3Response(int status, FileHandle handle, Nfs3FileAttributes attr, WccData
			 dirWcc)
			: base(status)
		{
			this.objFileHandle = handle;
			this.objAttr = attr;
			this.dirWcc = dirWcc;
		}

		public virtual FileHandle GetObjFileHandle()
		{
			return objFileHandle;
		}

		public virtual Nfs3FileAttributes GetObjAttr()
		{
			return objAttr;
		}

		public virtual WccData GetDirWcc()
		{
			return dirWcc;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.MKDIR3Response Deserialize(XDR 
			xdr)
		{
			int status = xdr.ReadInt();
			FileHandle objFileHandle = new FileHandle();
			Nfs3FileAttributes objAttr = null;
			WccData dirWcc;
			if (status == Nfs3Status.Nfs3Ok)
			{
				xdr.ReadBoolean();
				objFileHandle.Deserialize(xdr);
				xdr.ReadBoolean();
				objAttr = Nfs3FileAttributes.Deserialize(xdr);
			}
			dirWcc = WccData.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.MKDIR3Response(status, objFileHandle
				, objAttr, dirWcc);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteBoolean(true);
				// Handle follows
				objFileHandle.Serialize(@out);
				@out.WriteBoolean(true);
				// Attributes follow
				objAttr.Serialize(@out);
			}
			dirWcc.Serialize(@out);
			return @out;
		}
	}
}
