using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Record.Compiler
{
	/// <summary>A wrapper around StringBuffer that automatically does indentation</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class CodeBuffer
	{
		private static AList<char> startMarkers = new AList<char>();

		private static AList<char> endMarkers = new AList<char>();

		static CodeBuffer()
		{
			AddMarkers('{', '}');
			AddMarkers('(', ')');
		}

		internal static void AddMarkers(char ch1, char ch2)
		{
			startMarkers.AddItem(ch1);
			endMarkers.AddItem(ch2);
		}

		private int level = 0;

		private int numSpaces = 2;

		private bool firstChar = true;

		private StringBuilder sb;

		/// <summary>Creates a new instance of CodeBuffer</summary>
		internal CodeBuffer()
			: this(2, string.Empty)
		{
		}

		internal CodeBuffer(string s)
			: this(2, s)
		{
		}

		internal CodeBuffer(int numSpaces, string s)
		{
			sb = new StringBuilder();
			this.numSpaces = numSpaces;
			this.Append(s);
		}

		internal virtual void Append(string s)
		{
			int length = s.Length;
			for (int idx = 0; idx < length; idx++)
			{
				char ch = s[idx];
				Append(ch);
			}
		}

		internal virtual void Append(char ch)
		{
			if (endMarkers.Contains(ch))
			{
				level--;
			}
			if (firstChar)
			{
				for (int idx = 0; idx < level; idx++)
				{
					for (int num = 0; num < numSpaces; num++)
					{
						RawAppend(' ');
					}
				}
			}
			RawAppend(ch);
			firstChar = false;
			if (startMarkers.Contains(ch))
			{
				level++;
			}
			if (ch == '\n')
			{
				firstChar = true;
			}
		}

		private void RawAppend(char ch)
		{
			sb.Append(ch);
		}

		public override string ToString()
		{
			return sb.ToString();
		}
	}
}
