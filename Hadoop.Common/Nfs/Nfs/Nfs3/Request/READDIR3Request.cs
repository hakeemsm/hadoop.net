using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>READDIR3 Request</summary>
	public class READDIR3Request : RequestWithHandle
	{
		private readonly long cookie;

		private readonly long cookieVerf;

		private readonly int count;

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.READDIR3Request Deserialize(XDR 
			xdr)
		{
			FileHandle handle = ReadHandle(xdr);
			long cookie = xdr.ReadHyper();
			long cookieVerf = xdr.ReadHyper();
			int count = xdr.ReadInt();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.READDIR3Request(handle, cookie, cookieVerf
				, count);
		}

		public READDIR3Request(FileHandle handle, long cookie, long cookieVerf, int count
			)
			: base(handle)
		{
			this.cookie = cookie;
			this.cookieVerf = cookieVerf;
			this.count = count;
		}

		public virtual long GetCookie()
		{
			return this.cookie;
		}

		public virtual long GetCookieVerf()
		{
			return this.cookieVerf;
		}

		public virtual long GetCount()
		{
			return this.count;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteLongAsHyper(cookie);
			xdr.WriteLongAsHyper(cookieVerf);
			xdr.WriteInt(count);
		}
	}
}
