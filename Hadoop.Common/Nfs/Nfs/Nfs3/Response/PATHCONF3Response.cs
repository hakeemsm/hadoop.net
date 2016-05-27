using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>PATHCONF3 Response</summary>
	public class PATHCONF3Response : NFS3Response
	{
		private readonly Nfs3FileAttributes postOpAttr;

		private readonly int linkMax;

		private readonly int nameMax;

		private readonly bool noTrunc;

		private readonly bool chownRestricted;

		private readonly bool caseInsensitive;

		private readonly bool casePreserving;

		public PATHCONF3Response(int status)
			: this(status, new Nfs3FileAttributes(), 0, 0, false, false, false, false)
		{
		}

		public PATHCONF3Response(int status, Nfs3FileAttributes postOpAttr, int linkMax, 
			int nameMax, bool noTrunc, bool chownRestricted, bool caseInsensitive, bool casePreserving
			)
			: base(status)
		{
			/* The maximum number of hard links to an object. */
			/* The maximum length of a component of a filename. */
			/*
			* If TRUE, the server will reject any request that includes a name longer
			* than name_max with the error, NFS3ERR_NAMETOOLONG. If FALSE, any length
			* name over name_max bytes will be silently truncated to name_max bytes.
			*/
			/*
			* If TRUE, the server will reject any request to change either the owner or
			* the group associated with a file if the caller is not the privileged user.
			* (Uid 0.)
			*/
			/*
			* If TRUE, the server file system does not distinguish case when interpreting
			* filenames.
			*/
			/*
			* If TRUE, the server file system will preserve the case of a name during a
			* CREATE, MKDIR, MKNOD, SYMLINK, RENAME, or LINK operation.
			*/
			this.postOpAttr = postOpAttr;
			this.linkMax = linkMax;
			this.nameMax = nameMax;
			this.noTrunc = noTrunc;
			this.chownRestricted = chownRestricted;
			this.caseInsensitive = caseInsensitive;
			this.casePreserving = casePreserving;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.PATHCONF3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			xdr.ReadBoolean();
			Nfs3FileAttributes objPostOpAttr = Nfs3FileAttributes.Deserialize(xdr);
			int linkMax = 0;
			int nameMax = 0;
			bool noTrunc = false;
			bool chownRestricted = false;
			bool caseInsensitive = false;
			bool casePreserving = false;
			if (status == Nfs3Status.Nfs3Ok)
			{
				linkMax = xdr.ReadInt();
				nameMax = xdr.ReadInt();
				noTrunc = xdr.ReadBoolean();
				chownRestricted = xdr.ReadBoolean();
				caseInsensitive = xdr.ReadBoolean();
				casePreserving = xdr.ReadBoolean();
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.PATHCONF3Response(status, objPostOpAttr
				, linkMax, nameMax, noTrunc, chownRestricted, caseInsensitive, casePreserving);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			@out.WriteBoolean(true);
			postOpAttr.Serialize(@out);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteInt(linkMax);
				@out.WriteInt(nameMax);
				@out.WriteBoolean(noTrunc);
				@out.WriteBoolean(chownRestricted);
				@out.WriteBoolean(caseInsensitive);
				@out.WriteBoolean(casePreserving);
			}
			return @out;
		}
	}
}
