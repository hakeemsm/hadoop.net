using System;
using System.IO;
using System.Text;
using Org.Apache.Commons.IO;


namespace Org.Apache.Hadoop.Http
{
	/// <summary>This class is responsible for quoting HTML characters.</summary>
	public class HtmlQuoting
	{
		private static readonly byte[] ampBytes = Runtime.GetBytesForString("&amp;"
			, Charsets.Utf8);

		private static readonly byte[] aposBytes = Runtime.GetBytesForString("&apos;"
			, Charsets.Utf8);

		private static readonly byte[] gtBytes = Runtime.GetBytesForString("&gt;"
			, Charsets.Utf8);

		private static readonly byte[] ltBytes = Runtime.GetBytesForString("&lt;"
			, Charsets.Utf8);

		private static readonly byte[] quotBytes = Runtime.GetBytesForString("&quot;"
			, Charsets.Utf8);

		/// <summary>Does the given string need to be quoted?</summary>
		/// <param name="data">the string to check</param>
		/// <param name="off">the starting position</param>
		/// <param name="len">the number of bytes to check</param>
		/// <returns>does the string contain any of the active html characters?</returns>
		public static bool NeedsQuoting(byte[] data, int off, int len)
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
		public static bool NeedsQuoting(string str)
		{
			if (str == null)
			{
				return false;
			}
			byte[] bytes = Runtime.GetBytesForString(str, Charsets.Utf8);
			return NeedsQuoting(bytes, 0, bytes.Length);
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
		public static void QuoteHtmlChars(OutputStream output, byte[] buffer, int off, int
			 len)
		{
			for (int i = off; i < off + len; i++)
			{
				switch (buffer[i])
				{
					case (byte)('&'):
					{
						output.Write(ampBytes);
						break;
					}

					case (byte)('<'):
					{
						output.Write(ltBytes);
						break;
					}

					case (byte)('>'):
					{
						output.Write(gtBytes);
						break;
					}

					case (byte)('\''):
					{
						output.Write(aposBytes);
						break;
					}

					case (byte)('"'):
					{
						output.Write(quotBytes);
						break;
					}

					default:
					{
						output.Write(buffer, i, 1);
						break;
					}
				}
			}
		}

		/// <summary>Quote the given item to make it html-safe.</summary>
		/// <param name="item">the string to quote</param>
		/// <returns>the quoted string</returns>
		public static string QuoteHtmlChars(string item)
		{
			if (item == null)
			{
				return null;
			}
			byte[] bytes = Runtime.GetBytesForString(item, Charsets.Utf8);
			if (NeedsQuoting(bytes, 0, bytes.Length))
			{
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				try
				{
					QuoteHtmlChars(buffer, bytes, 0, bytes.Length);
					return buffer.ToString("UTF-8");
				}
				catch (IOException)
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
		public static OutputStream QuoteOutputStream(OutputStream @out)
		{
			return new _OutputStream_126(@out);
		}

		private sealed class _OutputStream_126 : OutputStream
		{
			public _OutputStream_126(OutputStream @out)
			{
				this.@out = @out;
				this.data = new byte[1];
			}

			private byte[] data;

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] data, int off, int len)
			{
				HtmlQuoting.QuoteHtmlChars(@out, data, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				this.data[0] = unchecked((byte)b);
				HtmlQuoting.QuoteHtmlChars(@out, this.data, 0, 1);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				@out.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				@out.Close();
			}

			private readonly OutputStream @out;
		}

		/// <summary>Remove HTML quoting from a string.</summary>
		/// <param name="item">the string to unquote</param>
		/// <returns>the unquoted string</returns>
		public static string UnquoteHtmlChars(string item)
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
			StringBuilder buffer = new StringBuilder();
			while (next != -1)
			{
				buffer.Append(Runtime.Substring(item, posn, next));
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
									throw new ArgumentException("Bad HTML quoting for " + Runtime.Substring(item
										, next, end));
								}
							}
						}
					}
				}
				posn = next;
				next = item.IndexOf('&', posn);
			}
			buffer.Append(Runtime.Substring(item, posn, len));
			return buffer.ToString();
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			foreach (string arg in args)
			{
				System.Console.Out.WriteLine("Original: " + arg);
				string quoted = QuoteHtmlChars(arg);
				System.Console.Out.WriteLine("Quoted: " + quoted);
				string unquoted = UnquoteHtmlChars(quoted);
				System.Console.Out.WriteLine("Unquoted: " + unquoted);
				System.Console.Out.WriteLine();
			}
		}
	}
}
