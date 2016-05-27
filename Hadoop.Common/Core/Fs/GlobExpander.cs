using System.Collections.Generic;
using System.IO;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.FS
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
		public static IList<string> Expand(string filePattern)
		{
			IList<string> fullyExpanded = new AList<string>();
			IList<GlobExpander.StringWithOffset> toExpand = new AList<GlobExpander.StringWithOffset
				>();
			toExpand.AddItem(new GlobExpander.StringWithOffset(filePattern, 0));
			while (!toExpand.IsEmpty())
			{
				GlobExpander.StringWithOffset path = toExpand.Remove(0);
				IList<GlobExpander.StringWithOffset> expanded = ExpandLeftmost(path);
				if (expanded == null)
				{
					fullyExpanded.AddItem(path.@string);
				}
				else
				{
					toExpand.AddRange(0, expanded);
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
		private static IList<GlobExpander.StringWithOffset> ExpandLeftmost(GlobExpander.StringWithOffset
			 filePatternWithOffset)
		{
			string filePattern = filePatternWithOffset.@string;
			int leftmost = LeftmostOuterCurlyContainingSlash(filePattern, filePatternWithOffset
				.offset);
			if (leftmost == -1)
			{
				return null;
			}
			int curlyOpen = 0;
			StringBuilder prefix = new StringBuilder(Sharpen.Runtime.Substring(filePattern, 0
				, leftmost));
			StringBuilder suffix = new StringBuilder();
			IList<string> alts = new AList<string>();
			StringBuilder alt = new StringBuilder();
			StringBuilder cur = prefix;
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
							throw new IOException("Illegal file pattern: " + "An escaped character does not present for glob "
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
									alts.AddItem(alt.ToString());
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
										alts.AddItem(alt.ToString());
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
			IList<GlobExpander.StringWithOffset> exp = new AList<GlobExpander.StringWithOffset
				>();
			foreach (string @string in alts)
			{
				exp.AddItem(new GlobExpander.StringWithOffset(prefix + @string + suffix, prefix.Length
					));
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
		private static int LeftmostOuterCurlyContainingSlash(string filePattern, int offset
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
						throw new IOException("Illegal file pattern: " + "An escaped character does not present for glob "
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
