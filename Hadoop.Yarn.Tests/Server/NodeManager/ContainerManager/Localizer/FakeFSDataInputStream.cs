using System.IO;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	/// <summary>mock streams in unit tests</summary>
	public class FakeFSDataInputStream : FilterInputStream, Seekable, PositionedReadable
	{
		public FakeFSDataInputStream(InputStream @in)
			: base(@in)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Seek(long pos)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetPos()
		{
			return -1;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool SeekToNewSource(long targetPos)
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual int Read(long position, byte[] buffer, int offset, int length)
		{
			return -1;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer, int offset, int length
			)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFully(long position, byte[] buffer)
		{
		}
	}
}
