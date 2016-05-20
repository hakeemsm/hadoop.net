using Sharpen;

namespace org.apache.hadoop.record.compiler
{
	/// <summary>A wrapper around StringBuffer that automatically does indentation</summary>
	[System.ObsoleteAttribute(@"Replaced by <a href=""http://hadoop.apache.org/avro/"">Avro</a>."
		)]
	public class CodeBuffer
	{
		private static System.Collections.Generic.List<char> startMarkers = new System.Collections.Generic.List
			<char>();

		private static System.Collections.Generic.List<char> endMarkers = new System.Collections.Generic.List
			<char>();

		static CodeBuffer()
		{
			addMarkers('{', '}');
			addMarkers('(', ')');
		}

		internal static void addMarkers(char ch1, char ch2)
		{
			startMarkers.add(ch1);
			endMarkers.add(ch2);
		}

		private int level = 0;

		private int numSpaces = 2;

		private bool firstChar = true;

		private System.Text.StringBuilder sb;

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
			sb = new System.Text.StringBuilder();
			this.numSpaces = numSpaces;
			this.append(s);
		}

		internal virtual void append(string s)
		{
			int length = s.Length;
			for (int idx = 0; idx < length; idx++)
			{
				char ch = s[idx];
				append(ch);
			}
		}

		internal virtual void append(char ch)
		{
			if (endMarkers.contains(ch))
			{
				level--;
			}
			if (firstChar)
			{
				for (int idx = 0; idx < level; idx++)
				{
					for (int num = 0; num < numSpaces; num++)
					{
						rawAppend(' ');
					}
				}
			}
			rawAppend(ch);
			firstChar = false;
			if (startMarkers.contains(ch))
			{
				level++;
			}
			if (ch == '\n')
			{
				firstChar = true;
			}
		}

		private void rawAppend(char ch)
		{
			sb.Append(ch);
		}

		public override string ToString()
		{
			return sb.ToString();
		}
	}
}
