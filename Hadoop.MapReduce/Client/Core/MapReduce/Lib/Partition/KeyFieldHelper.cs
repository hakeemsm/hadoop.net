using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	/// <summary>
	/// This is used in
	/// <see cref="KeyFieldBasedComparator{K, V}"/>
	/// &
	/// <see cref="KeyFieldBasedPartitioner{K2, V2}"/>
	/// . Defines all the methods
	/// for parsing key specifications. The key specification is of the form:
	/// -k pos1[,pos2], where pos is of the form f[.c][opts], where f is the number
	/// of the field to use, and c is the number of the first character from the
	/// beginning of the field. Fields and character posns are numbered starting
	/// with 1; a character position of zero in pos2 indicates the field's last
	/// character. If '.c' is omitted from pos1, it defaults to 1 (the beginning
	/// of the field); if omitted from pos2, it defaults to 0 (the end of the
	/// field). opts are ordering options (supported options are 'nr').
	/// </summary>
	internal class KeyFieldHelper
	{
		protected internal class KeyDescription
		{
			internal int beginFieldIdx = 1;

			internal int beginChar = 1;

			internal int endFieldIdx = 0;

			internal int endChar = 0;

			internal bool numeric;

			internal bool reverse;

			public override string ToString()
			{
				return "-k" + beginFieldIdx + "." + beginChar + "," + endFieldIdx + "." + endChar
					 + (numeric ? "n" : string.Empty) + (reverse ? "r" : string.Empty);
			}
		}

		private IList<KeyFieldHelper.KeyDescription> allKeySpecs = new AList<KeyFieldHelper.KeyDescription
			>();

		private byte[] keyFieldSeparator;

		private bool keySpecSeen = false;

		public virtual void SetKeyFieldSeparator(string keyFieldSeparator)
		{
			try
			{
				this.keyFieldSeparator = Sharpen.Runtime.GetBytesForString(keyFieldSeparator, "UTF-8"
					);
			}
			catch (UnsupportedEncodingException e)
			{
				throw new RuntimeException("The current system does not " + "support UTF-8 encoding!"
					, e);
			}
		}

		/// <summary>
		/// Required for backcompatibility with num.key.fields.for.partition in
		/// <see cref="KeyFieldBasedPartitioner{K2, V2}"/>
		/// 
		/// </summary>
		public virtual void SetKeyFieldSpec(int start, int end)
		{
			if (end >= start)
			{
				KeyFieldHelper.KeyDescription k = new KeyFieldHelper.KeyDescription();
				k.beginFieldIdx = start;
				k.endFieldIdx = end;
				keySpecSeen = true;
				allKeySpecs.AddItem(k);
			}
		}

		public virtual IList<KeyFieldHelper.KeyDescription> KeySpecs()
		{
			return allKeySpecs;
		}

		public virtual int[] GetWordLengths(byte[] b, int start, int end)
		{
			//Given a string like "hello how are you", it returns an array
			//like [4 5, 3, 3, 3], where the first element is the number of
			//fields
			if (!keySpecSeen)
			{
				//if there were no key specs, then the whole key is one word
				return new int[] { 1 };
			}
			int[] lengths = new int[10];
			int currLenLengths = lengths.Length;
			int idx = 1;
			int pos;
			while ((pos = UTF8ByteArrayUtils.FindBytes(b, start, end, keyFieldSeparator)) != 
				-1)
			{
				if (++idx == currLenLengths)
				{
					int[] temp = lengths;
					lengths = new int[(currLenLengths = currLenLengths * 2)];
					System.Array.Copy(temp, 0, lengths, 0, temp.Length);
				}
				lengths[idx - 1] = pos - start;
				start = pos + 1;
			}
			if (start != end)
			{
				lengths[idx] = end - start;
			}
			lengths[0] = idx;
			//number of words is the first element
			return lengths;
		}

		public virtual int GetStartOffset(byte[] b, int start, int end, int[] lengthIndices
			, KeyFieldHelper.KeyDescription k)
		{
			//if -k2.5,2 is the keyspec, the startChar is lengthIndices[1] + 5
			//note that the [0]'th element is the number of fields in the key
			if (lengthIndices[0] >= k.beginFieldIdx)
			{
				int position = 0;
				for (int i = 1; i < k.beginFieldIdx; i++)
				{
					position += lengthIndices[i] + keyFieldSeparator.Length;
				}
				if (position + k.beginChar <= (end - start))
				{
					return start + position + k.beginChar - 1;
				}
			}
			return -1;
		}

		public virtual int GetEndOffset(byte[] b, int start, int end, int[] lengthIndices
			, KeyFieldHelper.KeyDescription k)
		{
			//if -k2,2.8 is the keyspec, the endChar is lengthIndices[1] + 8
			//note that the [0]'th element is the number of fields in the key
			if (k.endFieldIdx == 0)
			{
				//there is no end field specified for this keyspec. So the remaining
				//part of the key is considered in its entirety.
				return end - 1;
			}
			if (lengthIndices[0] >= k.endFieldIdx)
			{
				int position = 0;
				int i;
				for (i = 1; i < k.endFieldIdx; i++)
				{
					position += lengthIndices[i] + keyFieldSeparator.Length;
				}
				if (k.endChar == 0)
				{
					position += lengthIndices[i];
				}
				if (position + k.endChar <= (end - start))
				{
					return start + position + k.endChar - 1;
				}
				return end - 1;
			}
			return end - 1;
		}

		public virtual void ParseOption(string option)
		{
			if (option == null || option.Equals(string.Empty))
			{
				//we will have only default comparison
				return;
			}
			StringTokenizer args = new StringTokenizer(option);
			KeyFieldHelper.KeyDescription global = new KeyFieldHelper.KeyDescription();
			while (args.HasMoreTokens())
			{
				string arg = args.NextToken();
				if (arg.Equals("-n"))
				{
					global.numeric = true;
				}
				if (arg.Equals("-r"))
				{
					global.reverse = true;
				}
				if (arg.Equals("-nr"))
				{
					global.numeric = true;
					global.reverse = true;
				}
				if (arg.StartsWith("-k"))
				{
					KeyFieldHelper.KeyDescription k = ParseKey(arg, args);
					if (k != null)
					{
						allKeySpecs.AddItem(k);
						keySpecSeen = true;
					}
				}
			}
			foreach (KeyFieldHelper.KeyDescription key in allKeySpecs)
			{
				if (!(key.reverse | key.numeric))
				{
					key.reverse = global.reverse;
					key.numeric = global.numeric;
				}
			}
			if (allKeySpecs.Count == 0)
			{
				allKeySpecs.AddItem(global);
			}
		}

		private KeyFieldHelper.KeyDescription ParseKey(string arg, StringTokenizer args)
		{
			//we allow for -k<arg> and -k <arg>
			string keyArgs = null;
			if (arg.Length == 2)
			{
				if (args.HasMoreTokens())
				{
					keyArgs = args.NextToken();
				}
			}
			else
			{
				keyArgs = Sharpen.Runtime.Substring(arg, 2);
			}
			if (keyArgs == null || keyArgs.Length == 0)
			{
				return null;
			}
			StringTokenizer st = new StringTokenizer(keyArgs, "nr.,", true);
			KeyFieldHelper.KeyDescription key = new KeyFieldHelper.KeyDescription();
			string token;
			//the key is of the form 1[.3][nr][,1.5][nr]
			if (st.HasMoreTokens())
			{
				token = st.NextToken();
				//the first token must be a number
				key.beginFieldIdx = System.Convert.ToInt32(token);
			}
			if (st.HasMoreTokens())
			{
				token = st.NextToken();
				if (token.Equals("."))
				{
					token = st.NextToken();
					key.beginChar = System.Convert.ToInt32(token);
					if (st.HasMoreTokens())
					{
						token = st.NextToken();
					}
					else
					{
						return key;
					}
				}
				do
				{
					if (token.Equals("n"))
					{
						key.numeric = true;
					}
					else
					{
						if (token.Equals("r"))
						{
							key.reverse = true;
						}
						else
						{
							break;
						}
					}
					if (st.HasMoreTokens())
					{
						token = st.NextToken();
					}
					else
					{
						return key;
					}
				}
				while (true);
				if (token.Equals(","))
				{
					token = st.NextToken();
					//the first token must be a number
					key.endFieldIdx = System.Convert.ToInt32(token);
					if (st.HasMoreTokens())
					{
						token = st.NextToken();
						if (token.Equals("."))
						{
							token = st.NextToken();
							key.endChar = System.Convert.ToInt32(token);
							if (st.HasMoreTokens())
							{
								token = st.NextToken();
							}
							else
							{
								return key;
							}
						}
						do
						{
							if (token.Equals("n"))
							{
								key.numeric = true;
							}
							else
							{
								if (token.Equals("r"))
								{
									key.reverse = true;
								}
								else
								{
									throw new ArgumentException("Invalid -k argument. " + "Must be of the form -k pos1,[pos2], where pos is of the form "
										 + "f[.c]nr");
								}
							}
							if (st.HasMoreTokens())
							{
								token = st.NextToken();
							}
							else
							{
								break;
							}
						}
						while (true);
					}
					return key;
				}
				throw new ArgumentException("Invalid -k argument. " + "Must be of the form -k pos1,[pos2], where pos is of the form "
					 + "f[.c]nr");
			}
			return key;
		}

		private void PrintKey(KeyFieldHelper.KeyDescription key)
		{
			System.Console.Out.WriteLine("key.beginFieldIdx: " + key.beginFieldIdx);
			System.Console.Out.WriteLine("key.beginChar: " + key.beginChar);
			System.Console.Out.WriteLine("key.endFieldIdx: " + key.endFieldIdx);
			System.Console.Out.WriteLine("key.endChar: " + key.endChar);
			System.Console.Out.WriteLine("key.numeric: " + key.numeric);
			System.Console.Out.WriteLine("key.reverse: " + key.reverse);
			System.Console.Out.WriteLine("parseKey over");
		}
	}
}
