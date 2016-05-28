using System.IO;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Wsrs
{
	public class InputStreamEntity : StreamingOutput
	{
		private InputStream @is;

		private long offset;

		private long len;

		public InputStreamEntity(InputStream @is, long offset, long len)
		{
			this.@is = @is;
			this.offset = offset;
			this.len = len;
		}

		public InputStreamEntity(InputStream @is)
			: this(@is, 0, -1)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(OutputStream os)
		{
			IOUtils.SkipFully(@is, offset);
			if (len == -1)
			{
				IOUtils.CopyBytes(@is, os, 4096, true);
			}
			else
			{
				IOUtils.CopyBytes(@is, os, len, true);
			}
		}
	}
}
