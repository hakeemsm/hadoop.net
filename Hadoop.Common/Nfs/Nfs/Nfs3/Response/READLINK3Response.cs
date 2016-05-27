using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>READLINK3 Response</summary>
	public class READLINK3Response : NFS3Response
	{
		private readonly Nfs3FileAttributes postOpSymlinkAttr;

		private readonly byte[] path;

		public READLINK3Response(int status)
			: this(status, new Nfs3FileAttributes(), new byte[0])
		{
		}

		public READLINK3Response(int status, Nfs3FileAttributes postOpAttr, byte[] path)
			: base(status)
		{
			this.postOpSymlinkAttr = postOpAttr;
			this.path = new byte[path.Length];
			System.Array.Copy(path, 0, this.path, 0, path.Length);
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.READLINK3Response Deserialize(XDR
			 xdr)
		{
			int status = xdr.ReadInt();
			xdr.ReadBoolean();
			Nfs3FileAttributes postOpSymlinkAttr = Nfs3FileAttributes.Deserialize(xdr);
			byte[] path = new byte[0];
			if (status == Nfs3Status.Nfs3Ok)
			{
				path = xdr.ReadVariableOpaque();
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.READLINK3Response(status, postOpSymlinkAttr
				, path);
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			@out.WriteBoolean(true);
			// Attribute follows
			postOpSymlinkAttr.Serialize(@out);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteVariableOpaque(path);
			}
			return @out;
		}
	}
}
