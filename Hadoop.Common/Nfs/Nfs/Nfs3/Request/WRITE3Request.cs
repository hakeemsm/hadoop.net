using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>WRITE3 Request</summary>
	public class WRITE3Request : RequestWithHandle
	{
		private long offset;

		private int count;

		private readonly Nfs3Constant.WriteStableHow stableHow;

		private readonly ByteBuffer data;

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.WRITE3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			long offset = xdr.ReadHyper();
			int count = xdr.ReadInt();
			Nfs3Constant.WriteStableHow stableHow = Nfs3Constant.WriteStableHow.FromValue(xdr
				.ReadInt());
			ByteBuffer data = ByteBuffer.Wrap(xdr.ReadFixedOpaque(xdr.ReadInt()));
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.WRITE3Request(handle, offset, count
				, stableHow, data);
		}

		public WRITE3Request(FileHandle handle, long offset, int count, Nfs3Constant.WriteStableHow
			 stableHow, ByteBuffer data)
			: base(handle)
		{
			this.offset = offset;
			this.count = count;
			this.stableHow = stableHow;
			this.data = data;
		}

		public virtual long GetOffset()
		{
			return this.offset;
		}

		public virtual void SetOffset(long offset)
		{
			this.offset = offset;
		}

		public virtual int GetCount()
		{
			return this.count;
		}

		public virtual void SetCount(int count)
		{
			this.count = count;
		}

		public virtual Nfs3Constant.WriteStableHow GetStableHow()
		{
			return this.stableHow;
		}

		public virtual ByteBuffer GetData()
		{
			return this.data;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteLongAsHyper(offset);
			xdr.WriteInt(count);
			xdr.WriteInt(stableHow.GetValue());
			xdr.WriteInt(count);
			xdr.WriteFixedOpaque(((byte[])data.Array()), count);
		}

		public override string ToString()
		{
			return string.Format("fileId: %d offset: %d count: %d stableHow: %s", handle.GetFileId
				(), offset, count, stableHow.ToString());
		}
	}
}
