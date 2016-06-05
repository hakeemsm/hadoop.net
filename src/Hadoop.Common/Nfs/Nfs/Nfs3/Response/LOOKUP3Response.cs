using System.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>LOOKUP3 Response</summary>
	public class LOOKUP3Response : NFS3Response
	{
		private readonly FileHandle fileHandle;

		private readonly Nfs3FileAttributes postOpObjAttr;

		private readonly Nfs3FileAttributes postOpDirAttr;

		public LOOKUP3Response(int status)
			: this(status, null, new Nfs3FileAttributes(), new Nfs3FileAttributes())
		{
		}

		public LOOKUP3Response(int status, FileHandle fileHandle, Nfs3FileAttributes postOpObjAttr
			, Nfs3FileAttributes postOpDirAttributes)
			: base(status)
		{
			// Can be null
			// Can be null
			this.fileHandle = fileHandle;
			this.postOpObjAttr = postOpObjAttr;
			this.postOpDirAttr = postOpDirAttributes;
		}

		/// <exception cref="System.IO.IOException"/>
		public LOOKUP3Response(XDR xdr)
			: base(-1)
		{
			fileHandle = new FileHandle();
			status = xdr.ReadInt();
			Nfs3FileAttributes objAttr = null;
			if (status == Nfs3Status.Nfs3Ok)
			{
				if (!fileHandle.Deserialize(xdr))
				{
					throw new IOException("can't deserialize file handle");
				}
				objAttr = xdr.ReadBoolean() ? Nfs3FileAttributes.Deserialize(xdr) : null;
			}
			postOpObjAttr = objAttr;
			postOpDirAttr = xdr.ReadBoolean() ? Nfs3FileAttributes.Deserialize(xdr) : null;
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			if (this.status == Nfs3Status.Nfs3Ok)
			{
				fileHandle.Serialize(@out);
				@out.WriteBoolean(true);
				// Attribute follows
				postOpObjAttr.Serialize(@out);
			}
			@out.WriteBoolean(true);
			// Attribute follows
			postOpDirAttr.Serialize(@out);
			return @out;
		}
	}
}
