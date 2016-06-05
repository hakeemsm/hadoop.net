using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>For sharing between the local and remote block reader implementations.</summary>
	internal class BlockReaderUtil
	{
		/* See {@link BlockReader#readAll(byte[], int, int)} */
		/// <exception cref="System.IO.IOException"/>
		public static int ReadAll(BlockReader reader, byte[] buf, int offset, int len)
		{
			int n = 0;
			for (; ; )
			{
				int nread = reader.Read(buf, offset + n, len - n);
				if (nread <= 0)
				{
					return (n == 0) ? nread : n;
				}
				n += nread;
				if (n >= len)
				{
					return n;
				}
			}
		}

		/* See {@link BlockReader#readFully(byte[], int, int)} */
		/// <exception cref="System.IO.IOException"/>
		public static void ReadFully(BlockReader reader, byte[] buf, int off, int len)
		{
			int toRead = len;
			while (toRead > 0)
			{
				int ret = reader.Read(buf, off, toRead);
				if (ret < 0)
				{
					throw new IOException("Premature EOF from inputStream");
				}
				toRead -= ret;
				off += ret;
			}
		}
	}
}
