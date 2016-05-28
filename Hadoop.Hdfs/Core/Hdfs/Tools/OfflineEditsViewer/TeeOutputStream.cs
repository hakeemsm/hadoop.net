using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineEditsViewer
{
	/// <summary>A TeeOutputStream writes its output to multiple output streams.</summary>
	public class TeeOutputStream : OutputStream
	{
		private readonly OutputStream[] outs;

		public TeeOutputStream(OutputStream[] outs)
		{
			this.outs = outs;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(int c)
		{
			foreach (OutputStream o in outs)
			{
				o.Write(c);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b)
		{
			foreach (OutputStream o in outs)
			{
				o.Write(b);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(byte[] b, int off, int len)
		{
			foreach (OutputStream o in outs)
			{
				o.Write(b, off, len);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			foreach (OutputStream o in outs)
			{
				o.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			foreach (OutputStream o in outs)
			{
				o.Flush();
			}
		}
	}
}
