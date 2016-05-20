using Sharpen;

namespace org.apache.hadoop.fs
{
	internal class GlobExpander
	{
		internal class StringWithOffset
		{
			internal string @string;

			internal int offset;

			public StringWithOffset(string @string, int offset)
				: base()
			{
				this.@string = @string;
				this.offset = offset;
			}
		}

		/// <summary>
		/// Expand globs in the given <code>filePattern</code> into a collection of
		/// file patterns so that in the expanded set no file pattern has a
		/// slash character ("/") in a curly bracket pair.
		/// </summary>
		/// <param name="filePattern"/>
		/// <returns>expanded file patterns</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public static System.Collections.Generic.IList<string> expand(string filePattern)
		{
			System.Collections.Generic.IList<string> fullyExpanded = new System.Collections.Generic.List
				<string>();
			System.Collections.Generic.IList<org.apache.hadoop.fs.GlobExpander.StringWithOffset
				> toExpand = new System.Collections.Generic.List<org.apache.hadoop.fs.GlobExpander.StringWithOffset
				>();
			toExpand.add(new org.apache.hadoop.fs.GlobExpander.StringWithOffset(filePattern, 
				0));
			while (!toExpand.isEmpty())
			{
				org.apache.hadoop.fs.GlobExpander.StringWithOffset path = toExpand.remove(0);
				System.Collections.Generic.IList<org.apache.hadoop.fs.GlobExpander.StringWithOffset
					> expanded = expandLeftmost(path);
				if (expanded == null)
				{
					fullyExpanded.add(path.@string);
				}
				else
				{
					toExpand.addAll(0, expanded);
				}
			}
			return fullyExpanded;
		}

		/// <summary>
		/// Expand the leftmost outer curly bracket pair containing a
		/// slash character ("/") in <code>filePattern</code>.
		/// </summary>
		/// <param name="filePattern"/>
		/// <returns>expanded file patterns</returns>
		/// <exception cref="System.IO.IOException"></exception>
		private static System.Collections.Generic.IList<org.apache.hadoop.fs.GlobExpander.StringWithOffset
			> expandLeftmost(org.apache.hadoop.fs.GlobExpander.StringWithOffset filePatternWithOffset
			)
		{
			string filePattern = filePatternWithOffset.@string;
			int leftmost = leftmostOuterCurlyContainingSlash(filePattern, filePatternWithOffset
				.offset);
			if (leftmost == -1)
			{
				return null;
			}
			int curlyOpen = 0;
			java.lang.StringBuilder prefix = new java.lang.StringBuilder(Sharpen.Runtime.substring
				(filePattern, 0, leftmost));
			java.lang.StringBuilder suffix = new java.lang.StringBuilder();
			System.Collections.Generic.IList<string> alts = new System.Collections.Generic.List
				<string>();
			java.lang.StringBuilder alt = new java.lang.StringBuilder();
			java.lang.StringBuilder cur = prefix;
			for (int i = leftmost; i < filePattern.Length; i++)
			{
				char c = filePattern[i];
				if (cur == suffix)
				{
					cur.Append(c);
				}
				else
				{
					if (c == '\\')
					{
						i++;
						if (i >= filePattern.Length)
						{
							throw new System.IO.IOException("Illegal file pattern: " + "An escaped character does not present for glob "
								 + filePattern + " at " + i);
						}
						c = filePattern[i];
						cur.Append(c);
					}
					else
					{
						if (c == '{')
						{
							if (curlyOpen++ == 0)
							{
								alt.Length = 0;
								cur = alt;
							}
							else
							{
								cur.Append(c);
							}
						}
						else
						{
							if (c == '}' && curlyOpen > 0)
							{
								if (--curlyOpen == 0)
								{
									alts.add(alt.ToString());
									alt.Length = 0;
									cur = suffix;
								}
								else
								{
									cur.Append(c);
								}
							}
							else
							{
								if (c == ',')
								{
									if (curlyOpen == 1)
									{
										alts.add(alt.ToString());
										alt.Length = 0;
									}
									else
									{
										cur.Append(c);
									}
								}
								else
								{
									cur.Append(c);
								}
							}
						}
					}
				}
			}
			System.Collections.Generic.IList<org.apache.hadoop.fs.GlobExpander.StringWithOffset
				> exp = new System.Collections.Generic.List<org.apache.hadoop.fs.GlobExpander.StringWithOffset
				>();
			foreach (string @string in alts)
			{
				exp.add(new org.apache.hadoop.fs.GlobExpander.StringWithOffset(prefix + @string +
					 suffix, prefix.Length));
			}
			return exp;
		}

		/// <summary>
		/// Finds the index of the leftmost opening curly bracket containing a
		/// slash character ("/") in <code>filePattern</code>.
		/// </summary>
		/// <param name="filePattern"/>
		/// <returns>
		/// the index of the leftmost opening curly bracket containing a
		/// slash character ("/"), or -1 if there is no such bracket
		/// </returns>
		/// <exception cref="System.IO.IOException"></exception>
		private static int leftmostOuterCurlyContainingSlash(string filePattern, int offset
			)
		{
			int curlyOpen = 0;
			int leftmost = -1;
			bool seenSlash = false;
			for (int i = offset; i < filePattern.Length; i++)
			{
				char c = filePattern[i];
				if (c == '\\')
				{
					i++;
					if (i >= filePattern.Length)
					{
						throw new System.IO.IOException("Illegal file pattern: " + "An escaped character does not present for glob "
							 + filePattern + " at " + i);
					}
				}
				else
				{
					if (c == '{')
					{
						if (curlyOpen++ == 0)
						{
							leftmost = i;
						}
					}
					else
					{
						if (c == '}' && curlyOpen > 0)
						{
							if (--curlyOpen == 0 && leftmost != -1 && seenSlash)
							{
								return leftmost;
							}
						}
						else
						{
							if (c == '/' && curlyOpen > 0)
							{
								seenSlash = true;
							}
						}
					}
				}
			}
			return -1;
		}
	}
}
