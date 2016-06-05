using System.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Join
{
	/// <summary>This class provides an implementation of ResetableIterator.</summary>
	/// <remarks>
	/// This class provides an implementation of ResetableIterator. This
	/// implementation uses a byte array to store elements added to it.
	/// </remarks>
	public class StreamBackedIterator<X> : ResetableIterator<X>
		where X : Writable
	{
		private class ReplayableByteInputStream : ByteArrayInputStream
		{
			public ReplayableByteInputStream(byte[] arr)
				: base(arr)
			{
			}

			public virtual void ResetStream()
			{
				mark = 0;
				Reset();
			}
		}

		private ByteArrayOutputStream outbuf = new ByteArrayOutputStream();

		private DataOutputStream outfbuf;

		private StreamBackedIterator.ReplayableByteInputStream inbuf;

		private DataInputStream infbuf;

		public StreamBackedIterator()
		{
			outfbuf = new DataOutputStream(outbuf);
		}

		public override bool HasNext()
		{
			return infbuf != null && inbuf.Available() > 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Next(X val)
		{
			if (HasNext())
			{
				inbuf.Mark(0);
				val.ReadFields(infbuf);
				return true;
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Replay(X val)
		{
			inbuf.Reset();
			if (0 == inbuf.Available())
			{
				return false;
			}
			val.ReadFields(infbuf);
			return true;
		}

		public override void Reset()
		{
			if (null != outfbuf)
			{
				inbuf = new StreamBackedIterator.ReplayableByteInputStream(outbuf.ToByteArray());
				infbuf = new DataInputStream(inbuf);
				outfbuf = null;
			}
			inbuf.ResetStream();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Add(X item)
		{
			item.Write(outfbuf);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (null != infbuf)
			{
				infbuf.Close();
			}
			if (null != outfbuf)
			{
				outfbuf.Close();
			}
		}

		public override void Clear()
		{
			if (null != inbuf)
			{
				inbuf.ResetStream();
			}
			outbuf.Reset();
			outfbuf = new DataOutputStream(outbuf);
		}
	}
}
