using Sharpen;

namespace org.apache.hadoop.util
{
	public class UTF8ByteArrayUtils
	{
		/// <summary>Find the first occurrence of the given byte b in a UTF-8 encoded string</summary>
		/// <param name="utf">a byte array containing a UTF-8 encoded string</param>
		/// <param name="start">starting offset</param>
		/// <param name="end">ending position</param>
		/// <param name="b">the byte to find</param>
		/// <returns>position that first byte occures otherwise -1</returns>
		public static int findByte(byte[] utf, int start, int end, byte b)
		{
			for (int i = start; i < end; i++)
			{
				if (utf[i] == b)
				{
					return i;
				}
			}
			return -1;
		}

		/// <summary>Find the first occurrence of the given bytes b in a UTF-8 encoded string
		/// 	</summary>
		/// <param name="utf">a byte array containing a UTF-8 encoded string</param>
		/// <param name="start">starting offset</param>
		/// <param name="end">ending position</param>
		/// <param name="b">the bytes to find</param>
		/// <returns>position that first byte occures otherwise -1</returns>
		public static int findBytes(byte[] utf, int start, int end, byte[] b)
		{
			int matchEnd = end - b.Length;
			for (int i = start; i <= matchEnd; i++)
			{
				bool matched = true;
				for (int j = 0; j < b.Length; j++)
				{
					if (utf[i + j] != b[j])
					{
						matched = false;
						break;
					}
				}
				if (matched)
				{
					return i;
				}
			}
			return -1;
		}

		/// <summary>Find the nth occurrence of the given byte b in a UTF-8 encoded string</summary>
		/// <param name="utf">a byte array containing a UTF-8 encoded string</param>
		/// <param name="start">starting offset</param>
		/// <param name="length">the length of byte array</param>
		/// <param name="b">the byte to find</param>
		/// <param name="n">the desired occurrence of the given byte</param>
		/// <returns>position that nth occurrence of the given byte if exists; otherwise -1</returns>
		public static int findNthByte(byte[] utf, int start, int length, byte b, int n)
		{
			int pos = -1;
			int nextStart = start;
			for (int i = 0; i < n; i++)
			{
				pos = findByte(utf, nextStart, length, b);
				if (pos < 0)
				{
					return pos;
				}
				nextStart = pos + 1;
			}
			return pos;
		}

		/// <summary>Find the nth occurrence of the given byte b in a UTF-8 encoded string</summary>
		/// <param name="utf">a byte array containing a UTF-8 encoded string</param>
		/// <param name="b">the byte to find</param>
		/// <param name="n">the desired occurrence of the given byte</param>
		/// <returns>position that nth occurrence of the given byte if exists; otherwise -1</returns>
		public static int findNthByte(byte[] utf, byte b, int n)
		{
			return findNthByte(utf, 0, utf.Length, b, n);
		}
	}
}
