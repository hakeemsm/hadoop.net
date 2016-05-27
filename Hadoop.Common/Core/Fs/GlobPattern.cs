using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>A class for POSIX glob pattern with brace expansions.</summary>
	public class GlobPattern
	{
		private const char Backslash = '\\';

		private Sharpen.Pattern compiled;

		private bool hasWildcard = false;

		/// <summary>Construct the glob pattern object with a glob pattern string</summary>
		/// <param name="globPattern">the glob pattern string</param>
		public GlobPattern(string globPattern)
		{
			Set(globPattern);
		}

		/// <returns>the compiled pattern</returns>
		public virtual Sharpen.Pattern Compiled()
		{
			return compiled;
		}

		/// <summary>Compile glob pattern string</summary>
		/// <param name="globPattern">the glob pattern</param>
		/// <returns>the pattern object</returns>
		public static Sharpen.Pattern Compile(string globPattern)
		{
			return new Org.Apache.Hadoop.FS.GlobPattern(globPattern).Compiled();
		}

		/// <summary>Match input against the compiled glob pattern</summary>
		/// <param name="s">input chars</param>
		/// <returns>true for successful matches</returns>
		public virtual bool Matches(CharSequence s)
		{
			return compiled.Matcher(s).Matches();
		}

		/// <summary>Set and compile a glob pattern</summary>
		/// <param name="glob">the glob pattern string</param>
		public virtual void Set(string glob)
		{
			StringBuilder regex = new StringBuilder();
			int setOpen = 0;
			int curlyOpen = 0;
			int len = glob.Length;
			hasWildcard = false;
			for (int i = 0; i < len; i++)
			{
				char c = glob[i];
				switch (c)
				{
					case Backslash:
					{
						if (++i >= len)
						{
							Error("Missing escaped character", glob, i);
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
						regex.Append(Backslash);
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
							Error("Unclosed character class", glob, i);
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
							regex.Append(Backslash);
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
				Error("Unclosed character class", glob, len);
			}
			if (curlyOpen > 0)
			{
				Error("Unclosed group", glob, len);
			}
			compiled = Sharpen.Pattern.Compile(regex.ToString());
		}

		/// <returns>true if this is a wildcard pattern (with special chars)</returns>
		public virtual bool HasWildcard()
		{
			return hasWildcard;
		}

		private static void Error(string message, string pattern, int pos)
		{
			throw new PatternSyntaxException(message, pattern, pos);
		}
	}
}
