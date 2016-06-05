using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>COMMIT3 Request</summary>
	public class COMMIT3Request : RequestWithHandle
	{
		private readonly long offset;

		private readonly int count;

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.COMMIT3Request Deserialize(XDR xdr
			)
		{
			FileHandle handle = ReadHandle(xdr);
			long offset = xdr.ReadHyper();
			int count = xdr.ReadInt();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.COMMIT3Request(handle, offset, count
				);
		}

		public COMMIT3Request(FileHandle handle, long offset, int count)
			: base(handle)
		{
			this.offset = offset;
			this.count = count;
		}

		public virtual long GetOffset()
		{
			return this.offset;
		}

		public virtual int GetCount()
		{
			return this.count;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteLongAsHyper(offset);
			xdr.WriteInt(count);
		}
	}
}
