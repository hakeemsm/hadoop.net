using Org.Apache.Hadoop.Nfs;
using Org.Apache.Hadoop.Oncrpc;
using Sharpen;

namespace Org.Apache.Hadoop.Nfs.Nfs3.Response
{
	/// <summary>WccAttr saves attributes used for weak cache consistency</summary>
	public class WccAttr
	{
		internal long size;

		internal NfsTime mtime;

		internal NfsTime ctime;

		// in milliseconds
		// in milliseconds
		public virtual long GetSize()
		{
			return size;
		}

		public virtual NfsTime GetMtime()
		{
			return mtime;
		}

		public virtual NfsTime GetCtime()
		{
			return ctime;
		}

		public WccAttr()
		{
			this.size = 0;
			mtime = null;
			ctime = null;
		}

		public WccAttr(long size, NfsTime mtime, NfsTime ctime)
		{
			this.size = size;
			this.mtime = mtime;
			this.ctime = ctime;
		}

		public static Org.Apache.Hadoop.Nfs.Nfs3.Response.WccAttr Deserialize(XDR xdr)
		{
			long size = xdr.ReadHyper();
			NfsTime mtime = NfsTime.Deserialize(xdr);
			NfsTime ctime = NfsTime.Deserialize(xdr);
			return new Org.Apache.Hadoop.Nfs.Nfs3.Response.WccAttr(size, mtime, ctime);
		}

		public virtual void Serialize(XDR @out)
		{
			@out.WriteLongAsHyper(size);
			if (mtime == null)
			{
				mtime = new NfsTime(0);
			}
			mtime.Serialize(@out);
			if (ctime == null)
			{
				ctime = new NfsTime(0);
			}
			ctime.Serialize(@out);
		}
	}
}
