using System;


namespace Org.Apache.Hadoop.FS.Permission
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
		public PermissionParser(string modeStr, Pattern symbolic, Pattern
			 octal)
		{
			Matcher matcher = null;
			if ((matcher = symbolic.Matcher(modeStr)).Find())
			{
				ApplyNormalPattern(modeStr, matcher);
			}
			else
			{
				if ((matcher = octal.Matcher(modeStr)).Matches())
				{
					ApplyOctalPattern(modeStr, matcher);
				}
				else
				{
					throw new ArgumentException(modeStr);
				}
			}
		}

		private void ApplyNormalPattern(string modeStr, Matcher matcher)
		{
			// Are there multiple permissions stored in one chmod?
			bool commaSeperated = false;
			for (int i = 0; i < 1 || matcher.End() < modeStr.Length; i++)
			{
				if (i > 0 && (!commaSeperated || !matcher.Find()))
				{
					throw new ArgumentException(modeStr);
				}
				/*
				* groups : 1 : [ugoa]* 2 : [+-=] 3 : [rwxXt]+ 4 : [,\s]*
				*/
				string str = matcher.Group(2);
				char type = str[str.Length - 1];
				bool user;
				bool group;
				bool others;
				bool stickyBit;
				user = group = others = stickyBit = false;
				foreach (char c in matcher.Group(1).ToCharArray())
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
							throw new RuntimeException("Unexpected");
						}
					}
				}
				if (!(user || group || others))
				{
					// same as specifying 'a'
					user = group = others = true;
				}
				short mode = 0;
				foreach (char c_1 in matcher.Group(3).ToCharArray())
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
							throw new RuntimeException("Unexpected");
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
				commaSeperated = matcher.Group(4).Contains(",");
			}
			symbolic = true;
		}

		private void ApplyOctalPattern(string modeStr, Matcher matcher)
		{
			userType = groupType = othersType = '=';
			// Check if sticky bit is specified
			string sb = matcher.Group(1);
			if (!sb.IsEmpty())
			{
				stickyMode = short.ValueOf(Runtime.Substring(sb, 0, 1));
				stickyBitType = '=';
			}
			string str = matcher.Group(2);
			userMode = short.ValueOf(Runtime.Substring(str, 0, 1));
			groupMode = short.ValueOf(Runtime.Substring(str, 1, 2));
			othersMode = short.ValueOf(Runtime.Substring(str, 2, 3));
		}

		protected internal virtual int CombineModes(int existing, bool exeOk)
		{
			return CombineModeSegments(stickyBitType, stickyMode, ((int)(((uint)existing) >> 
				9)), false) << 9 | CombineModeSegments(userType, userMode, ((int)(((uint)existing
				) >> 6)) & 7, exeOk) << 6 | CombineModeSegments(groupType, groupMode, ((int)(((uint
				)existing) >> 3)) & 7, exeOk) << 3 | CombineModeSegments(othersType, othersMode, 
				existing & 7, exeOk);
		}

		protected internal virtual int CombineModeSegments(char type, int mode, int existing
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
					throw new RuntimeException("Unexpected");
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
