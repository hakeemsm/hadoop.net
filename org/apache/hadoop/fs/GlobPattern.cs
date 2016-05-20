using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>A class for POSIX glob pattern with brace expansions.</summary>
	public class GlobPattern
	{
		private const char BACKSLASH = '\\';

		private java.util.regex.Pattern compiled;

		private bool hasWildcard = false;

		/// <summary>Construct the glob pattern object with a glob pattern string</summary>
		/// <param name="globPattern">the glob pattern string</param>
		public GlobPattern(string globPattern)
		{
			set(globPattern);
		}

		/// <returns>the compiled pattern</returns>
		public virtual java.util.regex.Pattern compiled()
		{
			return compiled;
		}

		/// <summary>Compile glob pattern string</summary>
		/// <param name="globPattern">the glob pattern</param>
		/// <returns>the pattern object</returns>
		public static java.util.regex.Pattern compile(string globPattern)
		{
			return new org.apache.hadoop.fs.GlobPattern(globPattern).compiled();
		}

		/// <summary>Match input against the compiled glob pattern</summary>
		/// <param name="s">input chars</param>
		/// <returns>true for successful matches</returns>
		public virtual bool matches(java.lang.CharSequence s)
		{
			return compiled.matcher(s).matches();
		}

		/// <summary>Set and compile a glob pattern</summary>
		/// <param name="glob">the glob pattern string</param>
		public virtual void set(string glob)
		{
			java.lang.StringBuilder regex = new java.lang.StringBuilder();
			int setOpen = 0;
			int curlyOpen = 0;
			int len = glob.Length;
			hasWildcard = false;
			for (int i = 0; i < len; i++)
			{
				char c = glob[i];
				switch (c)
				{
					case BACKSLASH:
					{
						if (++i >= len)
						{
							error("Missing escaped character", glob, i);
						}
						regex.Append(c).Append(glob[i]);
						continue;
					}

					case '.':
					case '$':
					case '(':
					case ')':
					case '|':
					case '+':
					{
						// escape regex special chars that are not glob special chars
						regex.Append(BACKSLASH);
						break;
					}

					case '*':
					{
						regex.Append('.');
						hasWildcard = true;
						break;
					}

					case '?':
					{
						regex.Append('.');
						hasWildcard = true;
						continue;
					}

					case '{':
					{
						// start of a group
						regex.Append("(?:");
						// non-capturing
						curlyOpen++;
						hasWildcard = true;
						continue;
					}

					case ',':
					{
						regex.Append(curlyOpen > 0 ? '|' : c);
						continue;
					}

					case '}':
					{
						if (curlyOpen > 0)
						{
							// end of a group
							curlyOpen--;
							regex.Append(")");
							continue;
						}
						break;
					}

					case '[':
					{
						if (setOpen > 0)
						{
							error("Unclosed character class", glob, i);
						}
						setOpen++;
						hasWildcard = true;
						break;
					}

					case '^':
					{
						// ^ inside [...] can be unescaped
						if (setOpen == 0)
						{
							regex.Append(BACKSLASH);
						}
						break;
					}

					case '!':
					{
						// [! needs to be translated to [^
						regex.Append(setOpen > 0 && '[' == glob[i - 1] ? '^' : '!');
						continue;
					}

					case ']':
					{
						// Many set errors like [][] could not be easily detected here,
						// as []], []-] and [-] are all valid POSIX glob and java regex.
						// We'll just let the regex compiler do the real work.
						setOpen = 0;
						break;
					}

					default:
					{
						break;
					}
				}
				regex.Append(c);
			}
			if (setOpen > 0)
			{
				error("Unclosed character class", glob, len);
			}
			if (curlyOpen > 0)
			{
				error("Unclosed group", glob, len);
			}
			compiled = java.util.regex.Pattern.compile(regex.ToString());
		}

		/// <returns>true if this is a wildcard pattern (with special chars)</returns>
		public virtual bool hasWildcard()
		{
			return hasWildcard;
		}

		private static void error(string message, string pattern, int pos)
		{
			throw new java.util.regex.PatternSyntaxException(message, pattern, pos);
		}
	}
}
