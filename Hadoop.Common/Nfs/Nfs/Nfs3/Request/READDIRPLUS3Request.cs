using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Oncrpc;


namespace Org.Apache.Hadoop.Nfs.Nfs3.Request
{
	/// <summary>READDIRPLUS3 Request</summary>
	public class READDIRPLUS3Request : RequestWithHandle
	{
		private readonly long cookie;

		private readonly long cookieVerf;

		private readonly int dirCount;

		private readonly int maxCount;

		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Nfs.Nfs3.Request.READDIRPLUS3Request Deserialize(
			XDR xdr)
		{
			FileHandle handle = ReadHandle(xdr);
			long cookie = xdr.ReadHyper();
			long cookieVerf = xdr.ReadHyper();
			int dirCount = xdr.ReadInt();
			int maxCount = xdr.ReadInt();
			return new Org.Apache.Hadoop.Nfs.Nfs3.Request.READDIRPLUS3Request(handle, cookie, 
				cookieVerf, dirCount, maxCount);
		}

		public READDIRPLUS3Request(FileHandle handle, long cookie, long cookieVerf, int dirCount
			, int maxCount)
			: base(handle)
		{
			this.cookie = cookie;
			this.cookieVerf = cookieVerf;
			this.dirCount = dirCount;
			this.maxCount = maxCount;
		}

		public virtual long GetCookie()
		{
			return this.cookie;
		}

		public virtual long GetCookieVerf()
		{
			return this.cookieVerf;
		}

		public virtual int GetDirCount()
		{
			return dirCount;
		}

		public virtual int GetMaxCount()
		{
			return maxCount;
		}

		public override void Serialize(XDR xdr)
		{
			handle.Serialize(xdr);
			xdr.WriteLongAsHyper(cookie);
			xdr.WriteLongAsHyper(cookieVerf);
			xdr.WriteInt(dirCount);
			xdr.WriteInt(maxCount);
		}
	}
}
