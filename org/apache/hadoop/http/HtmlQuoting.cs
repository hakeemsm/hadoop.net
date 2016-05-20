using Sharpen;

namespace org.apache.hadoop.http
{
	/// <summary>This class is responsible for quoting HTML characters.</summary>
	public class HtmlQuoting
	{
		private static readonly byte[] ampBytes = Sharpen.Runtime.getBytesForString("&amp;"
			, org.apache.commons.io.Charsets.UTF_8);

		private static readonly byte[] aposBytes = Sharpen.Runtime.getBytesForString("&apos;"
			, org.apache.commons.io.Charsets.UTF_8);

		private static readonly byte[] gtBytes = Sharpen.Runtime.getBytesForString("&gt;"
			, org.apache.commons.io.Charsets.UTF_8);

		private static readonly byte[] ltBytes = Sharpen.Runtime.getBytesForString("&lt;"
			, org.apache.commons.io.Charsets.UTF_8);

		private static readonly byte[] quotBytes = Sharpen.Runtime.getBytesForString("&quot;"
			, org.apache.commons.io.Charsets.UTF_8);

		/// <summary>Does the given string need to be quoted?</summary>
		/// <param name="data">the string to check</param>
		/// <param name="off">the starting position</param>
		/// <param name="len">the number of bytes to check</param>
		/// <returns>does the string contain any of the active html characters?</returns>
		public static bool needsQuoting(byte[] data, int off, int len)
		{
			for (int i = off; i < off + len; ++i)
			{
				switch (data[i])
				{
					case (byte)('&'):
					case (byte)('<'):
					case (byte)('>'):
					case (byte)('\''):
					case (byte)('"'):
					{
						return true;
					}

					default:
					{
						break;
					}
				}
			}
			return false;
		}

		/// <summary>Does the given string need to be quoted?</summary>
		/// <param name="str">the string to check</param>
		/// <returns>does the string contain any of the active html characters?</returns>
		public static bool needsQuoting(string str)
		{
			if (str == null)
			{
				return false;
			}
			byte[] bytes = Sharpen.Runtime.getBytesForString(str, org.apache.commons.io.Charsets
				.UTF_8);
			return needsQuoting(bytes, 0, bytes.Length);
		}

		/// <summary>
		/// Quote all of the active HTML characters in the given string as they
		/// are added to the buffer.
		/// </summary>
		/// <param name="output">the stream to write the output to</param>
		/// <param name="buffer">the byte array to take the characters from</param>
		/// <param name="off">the index of the first byte to quote</param>
		/// <param name="len">the number of bytes to quote</param>
		/// <exception cref="System.IO.IOException"/>
		public static void quoteHtmlChars(java.io.OutputStream output, byte[] buffer, int
			 off, int len)
		{
			for (int i = off; i < off + len; i++)
			{
				switch (buffer[i])
				{
					case (byte)('&'):
					{
						output.write(ampBytes);
						break;
					}

					case (byte)('<'):
					{
						output.write(ltBytes);
						break;
					}

					case (byte)('>'):
					{
						output.write(gtBytes);
						break;
					}

					case (byte)('\''):
					{
						output.write(aposBytes);
						break;
					}

					case (byte)('"'):
					{
						output.write(quotBytes);
						break;
					}

					default:
					{
						output.write(buffer, i, 1);
						break;
					}
				}
			}
		}

		/// <summary>Quote the given item to make it html-safe.</summary>
		/// <param name="item">the string to quote</param>
		/// <returns>the quoted string</returns>
		public static string quoteHtmlChars(string item)
		{
			if (item == null)
			{
				return null;
			}
			byte[] bytes = Sharpen.Runtime.getBytesForString(item, org.apache.commons.io.Charsets
				.UTF_8);
			if (needsQuoting(bytes, 0, bytes.Length))
			{
				java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
				try
				{
					quoteHtmlChars(buffer, bytes, 0, bytes.Length);
					return buffer.toString("UTF-8");
				}
				catch (System.IO.IOException)
				{
					// Won't happen, since it is a bytearrayoutputstream
					return null;
				}
			}
			else
			{
				return item;
			}
		}

		/// <summary>Return an output stream that quotes all of the output.</summary>
		/// <param name="out">the stream to write the quoted output to</param>
		/// <returns>a new stream that the application show write to</returns>
		/// <exception cref="System.IO.IOException">if the underlying output fails</exception>
		public static java.io.OutputStream quoteOutputStream(java.io.OutputStream @out)
		{
			return new _OutputStream_126(@out);
		}

		private sealed class _OutputStream_126 : java.io.OutputStream
		{
			public _OutputStream_126(java.io.OutputStream @out)
			{
				this.@out = @out;
				this.data = new byte[1];
			}

			private byte[] data;

			/// <exception cref="System.IO.IOException"/>
			public override void write(byte[] data, int off, int len)
			{
				org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(@out, data, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(int b)
			{
				this.data[0] = unchecked((byte)b);
				org.apache.hadoop.http.HtmlQuoting.quoteHtmlChars(@out, this.data, 0, 1);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
				@out.flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				@out.close();
			}

			private readonly java.io.OutputStream @out;
		}

		/// <summary>Remove HTML quoting from a string.</summary>
		/// <param name="item">the string to unquote</param>
		/// <returns>the unquoted string</returns>
		public static string unquoteHtmlChars(string item)
		{
			if (item == null)
			{
				return null;
			}
			int next = item.IndexOf('&');
			// nothing was quoted
			if (next == -1)
			{
				return item;
			}
			int len = item.Length;
			int posn = 0;
			java.lang.StringBuilder buffer = new java.lang.StringBuilder();
			while (next != -1)
			{
				buffer.Append(Sharpen.Runtime.substring(item, posn, next));
				if (item.StartsWith("&amp;", next))
				{
					buffer.Append('&');
					next += 5;
				}
				else
				{
					if (item.StartsWith("&apos;", next))
					{
						buffer.Append('\'');
						next += 6;
					}
					else
					{
						if (item.StartsWith("&gt;", next))
						{
							buffer.Append('>');
							next += 4;
						}
						else
						{
							if (item.StartsWith("&lt;", next))
							{
								buffer.Append('<');
								next += 4;
							}
							else
							{
								if (item.StartsWith("&quot;", next))
								{
									buffer.Append('"');
									next += 6;
								}
								else
								{
									int end = item.IndexOf(';', next) + 1;
									if (end == 0)
									{
										end = len;
									}
									throw new System.ArgumentException("Bad HTML quoting for " + Sharpen.Runtime.substring
										(item, next, end));
								}
							}
						}
					}
				}
				posn = next;
				next = item.IndexOf('&', posn);
			}
			buffer.Append(Sharpen.Runtime.substring(item, posn, len));
			return buffer.ToString();
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			foreach (string arg in args)
			{
				System.Console.Out.WriteLine("Original: " + arg);
				string quoted = quoteHtmlChars(arg);
				System.Console.Out.WriteLine("Quoted: " + quoted);
				string unquoted = unquoteHtmlChars(quoted);
				System.Console.Out.WriteLine("Unquoted: " + unquoted);
				System.Console.Out.WriteLine();
			}
		}
	}
}
