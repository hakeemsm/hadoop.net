using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>READ3 Response</summary>
	public class READ3Response : NFS3Response
	{
		private readonly Nfs3FileAttributes postOpAttr;

		private readonly int count;

		private readonly bool eof;

		private readonly ByteBuffer data;

		public READ3Response(int status)
			: this(status, new Nfs3FileAttributes(), 0, false, null)
		{
		}

		public READ3Response(int status, Nfs3FileAttributes postOpAttr, int count, bool eof
			, ByteBuffer data)
			: base(status)
		{
			// The real bytes of read data
			this.postOpAttr = postOpAttr;
			this.count = count;
			this.eof = eof;
			this.data = data;
		}

		public virtual Nfs3FileAttributes GetPostOpAttr()
		{
			return postOpAttr;
		}

		public virtual int GetCount()
		{
			return count;
		}

		public virtual bool IsEof()
		{
			return eof;
		}

		public virtual ByteBuffer GetData()
		{
			return data;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.READ3Response Deserialize(XDR xdr
			)
		{
			int status = xdr.ReadInt();
			xdr.ReadBoolean();
			Nfs3FileAttributes postOpAttr = Nfs3FileAttributes.Deserialize(xdr);
			int count = 0;
			bool eof = false;
			byte[] data = new byte[0];
			if (status == Nfs3Status.Nfs3Ok)
			{
				count = xdr.ReadInt();
				eof = xdr.ReadBoolean();
				int len = xdr.ReadInt();
				System.Diagnostics.Debug.Assert((len == count));
				data = xdr.ReadFixedOpaque(count);
			}
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.READ3Response(status, postOpAttr, 
				count, eof, ByteBuffer.Wrap(data));
		}

		public override XDR Serialize(XDR @out, int xid, Verifier verifier)
		{
			base.Serialize(@out, xid, verifier);
			@out.WriteBoolean(true);
			// Attribute follows
			postOpAttr.Serialize(@out);
			if (GetStatus() == Nfs3Status.Nfs3Ok)
			{
				@out.WriteInt(count);
				@out.WriteBoolean(eof);
				@out.WriteInt(count);
				@out.WriteFixedOpaque(((byte[])data.Array()), count);
			}
			return @out;
		}
	}
}
