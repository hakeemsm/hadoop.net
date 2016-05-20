using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>Base class for parsing either chmod permissions or umask permissions.</summary>
	/// <remarks>
	/// Base class for parsing either chmod permissions or umask permissions.
	/// Includes common code needed by either operation as implemented in
	/// UmaskParser and ChmodParser classes.
	/// </remarks>
	internal class PermissionParser
	{
		protected internal bool symbolic = false;

		protected internal short userMode;

		protected internal short groupMode;

		protected internal short othersMode;

		protected internal short stickyMode;

		protected internal char userType = '+';

		protected internal char groupType = '+';

		protected internal char othersType = '+';

		protected internal char stickyBitType = '+';

		/// <summary>Begin parsing permission stored in modeStr</summary>
		/// <param name="modeStr">Permission mode, either octal or symbolic</param>
		/// <param name="symbolic">Use-case specific symbolic pattern to match against</param>
		/// <exception cref="System.ArgumentException">if unable to parse modeStr</exception>
		public PermissionParser(string modeStr, java.util.regex.Pattern symbolic, java.util.regex.Pattern
			 octal)
		{
			java.util.regex.Matcher matcher = null;
			if ((matcher = symbolic.matcher(modeStr)).find())
			{
				applyNormalPattern(modeStr, matcher);
			}
			else
			{
				if ((matcher = octal.matcher(modeStr)).matches())
				{
					applyOctalPattern(modeStr, matcher);
				}
				else
				{
					throw new System.ArgumentException(modeStr);
				}
			}
		}

		private void applyNormalPattern(string modeStr, java.util.regex.Matcher matcher)
		{
			// Are there multiple permissions stored in one chmod?
			bool commaSeperated = false;
			for (int i = 0; i < 1 || matcher.end() < modeStr.Length; i++)
			{
				if (i > 0 && (!commaSeperated || !matcher.find()))
				{
					throw new System.ArgumentException(modeStr);
				}
				/*
				* groups : 1 : [ugoa]* 2 : [+-=] 3 : [rwxXt]+ 4 : [,\s]*
				*/
				string str = matcher.group(2);
				char type = str[str.Length - 1];
				bool user;
				bool group;
				bool others;
				bool stickyBit;
				user = group = others = stickyBit = false;
				foreach (char c in matcher.group(1).ToCharArray())
				{
					switch (c)
					{
						case 'u':
						{
							user = true;
							break;
						}

						case 'g':
						{
							group = true;
							break;
						}

						case 'o':
						{
							others = true;
							break;
						}

						case 'a':
						{
							break;
						}

						default:
						{
							throw new System.Exception("Unexpected");
						}
					}
				}
				if (!(user || group || others))
				{
					// same as specifying 'a'
					user = group = others = true;
				}
				short mode = 0;
				foreach (char c_1 in matcher.group(3).ToCharArray())
				{
					switch (c_1)
					{
						case 'r':
						{
							mode |= 4;
							break;
						}

						case 'w':
						{
							mode |= 2;
							break;
						}

						case 'x':
						{
							mode |= 1;
							break;
						}

						case 'X':
						{
							mode |= 8;
							break;
						}

						case 't':
						{
							stickyBit = true;
							break;
						}

						default:
						{
							throw new System.Exception("Unexpected");
						}
					}
				}
				if (user)
				{
					userMode = mode;
					userType = type;
				}
				if (group)
				{
					groupMode = mode;
					groupType = type;
				}
				if (others)
				{
					othersMode = mode;
					othersType = type;
					stickyMode = (short)(stickyBit ? 1 : 0);
					stickyBitType = type;
				}
				commaSeperated = matcher.group(4).contains(",");
			}
			symbolic = true;
		}

		private void applyOctalPattern(string modeStr, java.util.regex.Matcher matcher)
		{
			userType = groupType = othersType = '=';
			// Check if sticky bit is specified
			string sb = matcher.group(1);
			if (!sb.isEmpty())
			{
				stickyMode = short.valueOf(Sharpen.Runtime.substring(sb, 0, 1));
				stickyBitType = '=';
			}
			string str = matcher.group(2);
			userMode = short.valueOf(Sharpen.Runtime.substring(str, 0, 1));
			groupMode = short.valueOf(Sharpen.Runtime.substring(str, 1, 2));
			othersMode = short.valueOf(Sharpen.Runtime.substring(str, 2, 3));
		}

		protected internal virtual int combineModes(int existing, bool exeOk)
		{
			return combineModeSegments(stickyBitType, stickyMode, ((int)(((uint)existing) >> 
				9)), false) << 9 | combineModeSegments(userType, userMode, ((int)(((uint)existing
				) >> 6)) & 7, exeOk) << 6 | combineModeSegments(groupType, groupMode, ((int)(((uint
				)existing) >> 3)) & 7, exeOk) << 3 | combineModeSegments(othersType, othersMode, 
				existing & 7, exeOk);
		}

		protected internal virtual int combineModeSegments(char type, int mode, int existing
			, bool exeOk)
		{
			bool capX = false;
			if ((mode & 8) != 0)
			{
				// convert X to x;
				capX = true;
				mode &= ~8;
				mode |= 1;
			}
			switch (type)
			{
				case '+':
				{
					mode = mode | existing;
					break;
				}

				case '-':
				{
					mode = (~mode) & existing;
					break;
				}

				case '=':
				{
					break;
				}

				default:
				{
					throw new System.Exception("Unexpected");
				}
			}
			// if X is specified add 'x' only if exeOk or x was already set.
			if (capX && !exeOk && (mode & 1) != 0 && (existing & 1) == 0)
			{
				mode &= ~1;
			}
			// remove x
			return mode;
		}
	}
}
