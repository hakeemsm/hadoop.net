using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Base;
using Com.Google.Common.Net;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>General string utils</summary>
	public class StringUtils
	{
		/// <summary>Priority of the StringUtils shutdown hook.</summary>
		public const int ShutdownHookPriority = 0;

		/// <summary>
		/// Shell environment variables: $ followed by one letter or _ followed by
		/// multiple letters, numbers, or underscores.
		/// </summary>
		/// <remarks>
		/// Shell environment variables: $ followed by one letter or _ followed by
		/// multiple letters, numbers, or underscores.  The group captures the
		/// environment variable name without the leading $.
		/// </remarks>
		public static readonly Sharpen.Pattern ShellEnvVarPattern = Sharpen.Pattern.Compile
			("\\$([A-Za-z_]{1}[A-Za-z0-9_]*)");

		/// <summary>Windows environment variables: surrounded by %.</summary>
		/// <remarks>
		/// Windows environment variables: surrounded by %.  The group captures the
		/// environment variable name without the leading and trailing %.
		/// </remarks>
		public static readonly Sharpen.Pattern WinEnvVarPattern = Sharpen.Pattern.Compile
			("%(.*?)%");

		/// <summary>
		/// Regular expression that matches and captures environment variable names
		/// according to platform-specific rules.
		/// </summary>
		public static readonly Sharpen.Pattern EnvVarPattern = Shell.Windows ? WinEnvVarPattern
			 : ShellEnvVarPattern;

		/// <summary>Make a string representation of the exception.</summary>
		/// <param name="e">The exception to stringify</param>
		/// <returns>A string with exception name and call stack.</returns>
		public static string StringifyException(Exception e)
		{
			StringWriter stm = new StringWriter();
			PrintWriter wrt = new PrintWriter(stm);
			Sharpen.Runtime.PrintStackTrace(e, wrt);
			wrt.Close();
			return stm.ToString();
		}

		/// <summary>Given a full hostname, return the word upto the first dot.</summary>
		/// <param name="fullHostname">the full hostname</param>
		/// <returns>the hostname to the first dot</returns>
		public static string SimpleHostname(string fullHostname)
		{
			if (InetAddresses.IsInetAddress(fullHostname))
			{
				return fullHostname;
			}
			int offset = fullHostname.IndexOf('.');
			if (offset != -1)
			{
				return Sharpen.Runtime.Substring(fullHostname, 0, offset);
			}
			return fullHostname;
		}

		/// <summary>
		/// Given an integer, return a string that is in an approximate, but human
		/// readable format.
		/// </summary>
		/// <param name="number">the number to format</param>
		/// <returns>a human readable form of the integer</returns>
		[System.ObsoleteAttribute(@"use TraditionalBinaryPrefix.Long2String(long, string, int) ."
			)]
		public static string HumanReadableInt(long number)
		{
			return StringUtils.TraditionalBinaryPrefix.Long2String(number, string.Empty, 1);
		}

		/// <summary>The same as String.format(Locale.ENGLISH, format, objects).</summary>
		public static string Format(string format, params object[] objects)
		{
			return string.Format(Sharpen.Extensions.GetEnglishCulture(), format, objects);
		}

		/// <summary>Format a percentage for presentation to the user.</summary>
		/// <param name="fraction">the percentage as a fraction, e.g. 0.1 = 10%</param>
		/// <param name="decimalPlaces">the number of decimal places</param>
		/// <returns>a string representation of the percentage</returns>
		public static string FormatPercent(double fraction, int decimalPlaces)
		{
			return Format("%." + decimalPlaces + "f%%", fraction * 100);
		}

		/// <summary>Given an array of strings, return a comma-separated list of its elements.
		/// 	</summary>
		/// <param name="strs">Array of strings</param>
		/// <returns>
		/// Empty string if strs.length is 0, comma separated list of strings
		/// otherwise
		/// </returns>
		public static string ArrayToString(string[] strs)
		{
			if (strs.Length == 0)
			{
				return string.Empty;
			}
			StringBuilder sbuf = new StringBuilder();
			sbuf.Append(strs[0]);
			for (int idx = 1; idx < strs.Length; idx++)
			{
				sbuf.Append(",");
				sbuf.Append(strs[idx]);
			}
			return sbuf.ToString();
		}

		/// <summary>
		/// Given an array of bytes it will convert the bytes to a hex string
		/// representation of the bytes
		/// </summary>
		/// <param name="bytes"/>
		/// <param name="start">start index, inclusively</param>
		/// <param name="end">end index, exclusively</param>
		/// <returns>hex string representation of the byte array</returns>
		public static string ByteToHexString(byte[] bytes, int start, int end)
		{
			if (bytes == null)
			{
				throw new ArgumentException("bytes == null");
			}
			StringBuilder s = new StringBuilder();
			for (int i = start; i < end; i++)
			{
				s.Append(Format("%02x", bytes[i]));
			}
			return s.ToString();
		}

		/// <summary>Same as byteToHexString(bytes, 0, bytes.length).</summary>
		public static string ByteToHexString(byte[] bytes)
		{
			return ByteToHexString(bytes, 0, bytes.Length);
		}

		/// <summary>
		/// Given a hexstring this will return the byte array corresponding to the
		/// string
		/// </summary>
		/// <param name="hex">the hex String array</param>
		/// <returns>
		/// a byte array that is a hex string representation of the given
		/// string. The size of the byte array is therefore hex.length/2
		/// </returns>
		public static byte[] HexStringToByte(string hex)
		{
			byte[] bts = new byte[hex.Length / 2];
			for (int i = 0; i < bts.Length; i++)
			{
				bts[i] = unchecked((byte)System.Convert.ToInt32(Sharpen.Runtime.Substring(hex, 2 
					* i, 2 * i + 2), 16));
			}
			return bts;
		}

		/// <param name="uris"/>
		public static string UriToString(URI[] uris)
		{
			if (uris == null)
			{
				return null;
			}
			StringBuilder ret = new StringBuilder(uris[0].ToString());
			for (int i = 1; i < uris.Length; i++)
			{
				ret.Append(",");
				ret.Append(uris[i].ToString());
			}
			return ret.ToString();
		}

		/// <param name="str">The string array to be parsed into an URI array.</param>
		/// <returns>
		/// <tt>null</tt> if str is <tt>null</tt>, else the URI array
		/// equivalent to str.
		/// </returns>
		/// <exception cref="System.ArgumentException">If any string in str violates RFC&nbsp;2396.
		/// 	</exception>
		public static URI[] StringToURI(string[] str)
		{
			if (str == null)
			{
				return null;
			}
			URI[] uris = new URI[str.Length];
			for (int i = 0; i < str.Length; i++)
			{
				try
				{
					uris[i] = new URI(str[i]);
				}
				catch (URISyntaxException ur)
				{
					throw new ArgumentException("Failed to create uri for " + str[i], ur);
				}
			}
			return uris;
		}

		/// <param name="str"/>
		public static Path[] StringToPath(string[] str)
		{
			if (str == null)
			{
				return null;
			}
			Path[] p = new Path[str.Length];
			for (int i = 0; i < str.Length; i++)
			{
				p[i] = new Path(str[i]);
			}
			return p;
		}

		/// <summary>
		/// Given a finish and start time in long milliseconds, returns a
		/// String in the format Xhrs, Ymins, Z sec, for the time difference between two times.
		/// </summary>
		/// <remarks>
		/// Given a finish and start time in long milliseconds, returns a
		/// String in the format Xhrs, Ymins, Z sec, for the time difference between two times.
		/// If finish time comes before start time then negative valeus of X, Y and Z wil return.
		/// </remarks>
		/// <param name="finishTime">finish time</param>
		/// <param name="startTime">start time</param>
		public static string FormatTimeDiff(long finishTime, long startTime)
		{
			long timeDiff = finishTime - startTime;
			return FormatTime(timeDiff);
		}

		/// <summary>
		/// Given the time in long milliseconds, returns a
		/// String in the format Xhrs, Ymins, Z sec.
		/// </summary>
		/// <param name="timeDiff">The time difference to format</param>
		public static string FormatTime(long timeDiff)
		{
			StringBuilder buf = new StringBuilder();
			long hours = timeDiff / (60 * 60 * 1000);
			long rem = (timeDiff % (60 * 60 * 1000));
			long minutes = rem / (60 * 1000);
			rem = rem % (60 * 1000);
			long seconds = rem / 1000;
			if (hours != 0)
			{
				buf.Append(hours);
				buf.Append("hrs, ");
			}
			if (minutes != 0)
			{
				buf.Append(minutes);
				buf.Append("mins, ");
			}
			// return "0sec if no difference
			buf.Append(seconds);
			buf.Append("sec");
			return buf.ToString();
		}

		/// <summary>
		/// Formats time in ms and appends difference (finishTime - startTime)
		/// as returned by formatTimeDiff().
		/// </summary>
		/// <remarks>
		/// Formats time in ms and appends difference (finishTime - startTime)
		/// as returned by formatTimeDiff().
		/// If finish time is 0, empty string is returned, if start time is 0
		/// then difference is not appended to return value.
		/// </remarks>
		/// <param name="dateFormat">date format to use</param>
		/// <param name="finishTime">fnish time</param>
		/// <param name="startTime">start time</param>
		/// <returns>formatted value.</returns>
		public static string GetFormattedTimeWithDiff(DateFormat dateFormat, long finishTime
			, long startTime)
		{
			StringBuilder buf = new StringBuilder();
			if (0 != finishTime)
			{
				buf.Append(dateFormat.Format(Sharpen.Extensions.CreateDate(finishTime)));
				if (0 != startTime)
				{
					buf.Append(" (" + FormatTimeDiff(finishTime, startTime) + ")");
				}
			}
			return buf.ToString();
		}

		/// <summary>Returns an arraylist of strings.</summary>
		/// <param name="str">the comma seperated string values</param>
		/// <returns>the arraylist of the comma seperated string values</returns>
		public static string[] GetStrings(string str)
		{
			ICollection<string> values = GetStringCollection(str);
			if (values.Count == 0)
			{
				return null;
			}
			return Sharpen.Collections.ToArray(values, new string[values.Count]);
		}

		/// <summary>Returns a collection of strings.</summary>
		/// <param name="str">comma seperated string values</param>
		/// <returns>an <code>ArrayList</code> of string values</returns>
		public static ICollection<string> GetStringCollection(string str)
		{
			string delim = ",";
			return GetStringCollection(str, delim);
		}

		/// <summary>Returns a collection of strings.</summary>
		/// <param name="str">String to parse</param>
		/// <param name="delim">delimiter to separate the values</param>
		/// <returns>Collection of parsed elements.</returns>
		public static ICollection<string> GetStringCollection(string str, string delim)
		{
			IList<string> values = new AList<string>();
			if (str == null)
			{
				return values;
			}
			StringTokenizer tokenizer = new StringTokenizer(str, delim);
			while (tokenizer.HasMoreTokens())
			{
				values.AddItem(tokenizer.NextToken());
			}
			return values;
		}

		/// <summary>Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
		/// 	</summary>
		/// <remarks>
		/// Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
		/// Duplicate and empty values are removed.
		/// </remarks>
		/// <param name="str">a comma separated <String> with values</param>
		/// <returns>a <code>Collection</code> of <code>String</code> values</returns>
		public static ICollection<string> GetTrimmedStringCollection(string str)
		{
			ICollection<string> set = new LinkedHashSet<string>(Arrays.AsList(GetTrimmedStrings
				(str)));
			set.Remove(string.Empty);
			return set;
		}

		/// <summary>Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
		/// 	</summary>
		/// <param name="str">a comma separated <String> with values</param>
		/// <returns>an array of <code>String</code> values</returns>
		public static string[] GetTrimmedStrings(string str)
		{
			if (null == str || str.Trim().IsEmpty())
			{
				return emptyStringArray;
			}
			return str.Trim().Split("\\s*,\\s*");
		}

		/// <summary>Trims all the strings in a Collection<String> and returns a Set<String>.
		/// 	</summary>
		/// <param name="strings"/>
		/// <returns/>
		public static ICollection<string> GetTrimmedStrings(ICollection<string> strings)
		{
			ICollection<string> trimmedStrings = new HashSet<string>();
			foreach (string @string in strings)
			{
				trimmedStrings.AddItem(@string.Trim());
			}
			return trimmedStrings;
		}

		public static readonly string[] emptyStringArray = new string[] {  };

		public const char Comma = ',';

		public const string CommaStr = ",";

		public const char EscapeChar = '\\';

		/// <summary>Split a string using the default separator</summary>
		/// <param name="str">a string that may have escaped separator</param>
		/// <returns>an array of strings</returns>
		public static string[] Split(string str)
		{
			return Split(str, EscapeChar, Comma);
		}

		/// <summary>Split a string using the given separator</summary>
		/// <param name="str">a string that may have escaped separator</param>
		/// <param name="escapeChar">a char that be used to escape the separator</param>
		/// <param name="separator">a separator char</param>
		/// <returns>an array of strings</returns>
		public static string[] Split(string str, char escapeChar, char separator)
		{
			if (str == null)
			{
				return null;
			}
			AList<string> strList = new AList<string>();
			StringBuilder split = new StringBuilder();
			int index = 0;
			while ((index = FindNext(str, separator, escapeChar, index, split)) >= 0)
			{
				++index;
				// move over the separator for next search
				strList.AddItem(split.ToString());
				split.Length = 0;
			}
			// reset the buffer 
			strList.AddItem(split.ToString());
			// remove trailing empty split(s)
			int last = strList.Count;
			// last split
			while (--last >= 0 && string.Empty.Equals(strList[last]))
			{
				strList.Remove(last);
			}
			return Sharpen.Collections.ToArray(strList, new string[strList.Count]);
		}

		/// <summary>Split a string using the given separator, with no escaping performed.</summary>
		/// <param name="str">a string to be split. Note that this may not be null.</param>
		/// <param name="separator">a separator char</param>
		/// <returns>an array of strings</returns>
		public static string[] Split(string str, char separator)
		{
			// String.split returns a single empty result for splitting the empty
			// string.
			if (str.IsEmpty())
			{
				return new string[] { string.Empty };
			}
			AList<string> strList = new AList<string>();
			int startIndex = 0;
			int nextIndex = 0;
			while ((nextIndex = str.IndexOf(separator, startIndex)) != -1)
			{
				strList.AddItem(Sharpen.Runtime.Substring(str, startIndex, nextIndex));
				startIndex = nextIndex + 1;
			}
			strList.AddItem(Sharpen.Runtime.Substring(str, startIndex));
			// remove trailing empty split(s)
			int last = strList.Count;
			// last split
			while (--last >= 0 && string.Empty.Equals(strList[last]))
			{
				strList.Remove(last);
			}
			return Sharpen.Collections.ToArray(strList, new string[strList.Count]);
		}

		/// <summary>
		/// Finds the first occurrence of the separator character ignoring the escaped
		/// separators starting from the index.
		/// </summary>
		/// <remarks>
		/// Finds the first occurrence of the separator character ignoring the escaped
		/// separators starting from the index. Note the substring between the index
		/// and the position of the separator is passed.
		/// </remarks>
		/// <param name="str">the source string</param>
		/// <param name="separator">the character to find</param>
		/// <param name="escapeChar">character used to escape</param>
		/// <param name="start">from where to search</param>
		/// <param name="split">used to pass back the extracted string</param>
		public static int FindNext(string str, char separator, char escapeChar, int start
			, StringBuilder split)
		{
			int numPreEscapes = 0;
			for (int i = start; i < str.Length; i++)
			{
				char curChar = str[i];
				if (numPreEscapes == 0 && curChar == separator)
				{
					// separator 
					return i;
				}
				else
				{
					split.Append(curChar);
					numPreEscapes = (curChar == escapeChar) ? (++numPreEscapes) % 2 : 0;
				}
			}
			return -1;
		}

		/// <summary>Escape commas in the string using the default escape char</summary>
		/// <param name="str">a string</param>
		/// <returns>an escaped string</returns>
		public static string EscapeString(string str)
		{
			return EscapeString(str, EscapeChar, Comma);
		}

		/// <summary>
		/// Escape <code>charToEscape</code> in the string
		/// with the escape char <code>escapeChar</code>
		/// </summary>
		/// <param name="str">string</param>
		/// <param name="escapeChar">escape char</param>
		/// <param name="charToEscape">the char to be escaped</param>
		/// <returns>an escaped string</returns>
		public static string EscapeString(string str, char escapeChar, char charToEscape)
		{
			return EscapeString(str, escapeChar, new char[] { charToEscape });
		}

		// check if the character array has the character 
		private static bool HasChar(char[] chars, char character)
		{
			foreach (char target in chars)
			{
				if (character == target)
				{
					return true;
				}
			}
			return false;
		}

		/// <param name="charsToEscape">array of characters to be escaped</param>
		public static string EscapeString(string str, char escapeChar, char[] charsToEscape
			)
		{
			if (str == null)
			{
				return null;
			}
			StringBuilder result = new StringBuilder();
			for (int i = 0; i < str.Length; i++)
			{
				char curChar = str[i];
				if (curChar == escapeChar || HasChar(charsToEscape, curChar))
				{
					// special char
					result.Append(escapeChar);
				}
				result.Append(curChar);
			}
			return result.ToString();
		}

		/// <summary>Unescape commas in the string using the default escape char</summary>
		/// <param name="str">a string</param>
		/// <returns>an unescaped string</returns>
		public static string UnEscapeString(string str)
		{
			return UnEscapeString(str, EscapeChar, Comma);
		}

		/// <summary>
		/// Unescape <code>charToEscape</code> in the string
		/// with the escape char <code>escapeChar</code>
		/// </summary>
		/// <param name="str">string</param>
		/// <param name="escapeChar">escape char</param>
		/// <param name="charToEscape">the escaped char</param>
		/// <returns>an unescaped string</returns>
		public static string UnEscapeString(string str, char escapeChar, char charToEscape
			)
		{
			return UnEscapeString(str, escapeChar, new char[] { charToEscape });
		}

		/// <param name="charsToEscape">array of characters to unescape</param>
		public static string UnEscapeString(string str, char escapeChar, char[] charsToEscape
			)
		{
			if (str == null)
			{
				return null;
			}
			StringBuilder result = new StringBuilder(str.Length);
			bool hasPreEscape = false;
			for (int i = 0; i < str.Length; i++)
			{
				char curChar = str[i];
				if (hasPreEscape)
				{
					if (curChar != escapeChar && !HasChar(charsToEscape, curChar))
					{
						// no special char
						throw new ArgumentException("Illegal escaped string " + str + " unescaped " + escapeChar
							 + " at " + (i - 1));
					}
					// otherwise discard the escape char
					result.Append(curChar);
					hasPreEscape = false;
				}
				else
				{
					if (HasChar(charsToEscape, curChar))
					{
						throw new ArgumentException("Illegal escaped string " + str + " unescaped " + curChar
							 + " at " + i);
					}
					else
					{
						if (curChar == escapeChar)
						{
							hasPreEscape = true;
						}
						else
						{
							result.Append(curChar);
						}
					}
				}
			}
			if (hasPreEscape)
			{
				throw new ArgumentException("Illegal escaped string " + str + ", not expecting " 
					+ escapeChar + " in the end.");
			}
			return result.ToString();
		}

		/// <summary>Return a message for logging.</summary>
		/// <param name="prefix">prefix keyword for the message</param>
		/// <param name="msg">content of the message</param>
		/// <returns>a message for logging</returns>
		private static string ToStartupShutdownString(string prefix, string[] msg)
		{
			StringBuilder b = new StringBuilder(prefix);
			b.Append("\n/************************************************************");
			foreach (string s in msg)
			{
				b.Append("\n" + prefix + s);
			}
			b.Append("\n************************************************************/");
			return b.ToString();
		}

		/// <summary>Print a log message for starting up and shutting down</summary>
		/// <param name="clazz">the class of the server</param>
		/// <param name="args">arguments</param>
		/// <param name="Log">the target log object</param>
		public static void StartupShutdownMessage(Type clazz, string[] args, Log Log)
		{
			StartupShutdownMessage(clazz, args, LogAdapter.Create(Log));
		}

		/// <summary>Print a log message for starting up and shutting down</summary>
		/// <param name="clazz">the class of the server</param>
		/// <param name="args">arguments</param>
		/// <param name="Log">the target log object</param>
		public static void StartupShutdownMessage(Type clazz, string[] args, Logger Log)
		{
			StartupShutdownMessage(clazz, args, LogAdapter.Create(Log));
		}

		internal static void StartupShutdownMessage(Type clazz, string[] args, LogAdapter
			 Log)
		{
			string hostname = NetUtils.GetHostname();
			string classname = clazz.Name;
			Log.Info(ToStartupShutdownString("STARTUP_MSG: ", new string[] { "Starting " + classname
				, "  host = " + hostname, "  args = " + Arrays.AsList(args), "  version = " + VersionInfo
				.GetVersion(), "  classpath = " + Runtime.GetProperty("java.class.path"), "  build = "
				 + VersionInfo.GetUrl() + " -r " + VersionInfo.GetRevision() + "; compiled by '"
				 + VersionInfo.GetUser() + "' on " + VersionInfo.GetDate(), "  java = " + Runtime
				.GetProperty("java.version") }));
			if (SystemUtils.IsOsUnix)
			{
				try
				{
					SignalLogger.Instance.Register(Log);
				}
				catch (Exception t)
				{
					Log.Warn("failed to register any UNIX signal loggers: ", t);
				}
			}
			ShutdownHookManager.Get().AddShutdownHook(new _Runnable_673(Log, classname, hostname
				), ShutdownHookPriority);
		}

		private sealed class _Runnable_673 : Runnable
		{
			public _Runnable_673(LogAdapter Log, string classname, string hostname)
			{
				this.Log = Log;
				this.classname = classname;
				this.hostname = hostname;
			}

			public void Run()
			{
				Log.Info(StringUtils.ToStartupShutdownString("SHUTDOWN_MSG: ", new string[] { "Shutting down "
					 + classname + " at " + hostname }));
			}

			private readonly LogAdapter Log;

			private readonly string classname;

			private readonly string hostname;
		}

		/// <summary>
		/// The traditional binary prefixes, kilo, mega, ..., exa,
		/// which can be represented by a 64-bit integer.
		/// </summary>
		/// <remarks>
		/// The traditional binary prefixes, kilo, mega, ..., exa,
		/// which can be represented by a 64-bit integer.
		/// TraditionalBinaryPrefix symbol are case insensitive.
		/// </remarks>
		[System.Serializable]
		public sealed class TraditionalBinaryPrefix
		{
			public static readonly StringUtils.TraditionalBinaryPrefix Kilo = new StringUtils.TraditionalBinaryPrefix
				(10);

			public static readonly StringUtils.TraditionalBinaryPrefix Mega = new StringUtils.TraditionalBinaryPrefix
				(StringUtils.TraditionalBinaryPrefix.Kilo.bitShift + 10);

			public static readonly StringUtils.TraditionalBinaryPrefix Giga = new StringUtils.TraditionalBinaryPrefix
				(StringUtils.TraditionalBinaryPrefix.Mega.bitShift + 10);

			public static readonly StringUtils.TraditionalBinaryPrefix Tera = new StringUtils.TraditionalBinaryPrefix
				(StringUtils.TraditionalBinaryPrefix.Giga.bitShift + 10);

			public static readonly StringUtils.TraditionalBinaryPrefix Peta = new StringUtils.TraditionalBinaryPrefix
				(StringUtils.TraditionalBinaryPrefix.Tera.bitShift + 10);

			public static readonly StringUtils.TraditionalBinaryPrefix Exa = new StringUtils.TraditionalBinaryPrefix
				(StringUtils.TraditionalBinaryPrefix.Peta.bitShift + 10);

			public readonly long value;

			public readonly char symbol;

			public readonly int bitShift;

			public readonly long bitMask;

			private TraditionalBinaryPrefix(int bitShift)
			{
				this.bitShift = bitShift;
				this.value = 1L << bitShift;
				this.bitMask = this.value - 1L;
				this.symbol = ToString()[0];
			}

			/// <returns>The TraditionalBinaryPrefix object corresponding to the symbol.</returns>
			public static StringUtils.TraditionalBinaryPrefix ValueOf(char symbol)
			{
				symbol = System.Char.ToUpper(symbol);
				foreach (StringUtils.TraditionalBinaryPrefix prefix in StringUtils.TraditionalBinaryPrefix
					.Values())
				{
					if (symbol == prefix.symbol)
					{
						return prefix;
					}
				}
				throw new ArgumentException("Unknown symbol '" + symbol + "'");
			}

			/// <summary>Convert a string to long.</summary>
			/// <remarks>
			/// Convert a string to long.
			/// The input string is first be trimmed
			/// and then it is parsed with traditional binary prefix.
			/// For example,
			/// "-1230k" will be converted to -1230 * 1024 = -1259520;
			/// "891g" will be converted to 891 * 1024^3 = 956703965184;
			/// </remarks>
			/// <param name="s">input string</param>
			/// <returns>a long value represented by the input string.</returns>
			public static long String2long(string s)
			{
				s = s.Trim();
				int lastpos = s.Length - 1;
				char lastchar = s[lastpos];
				if (char.IsDigit(lastchar))
				{
					return long.Parse(s);
				}
				else
				{
					long prefix;
					try
					{
						prefix = StringUtils.TraditionalBinaryPrefix.ValueOf(lastchar).value;
					}
					catch (ArgumentException)
					{
						throw new ArgumentException("Invalid size prefix '" + lastchar + "' in '" + s + "'. Allowed prefixes are k, m, g, t, p, e(case insensitive)"
							);
					}
					long num = long.Parse(Sharpen.Runtime.Substring(s, 0, lastpos));
					if (num > (long.MaxValue / prefix) || num < (long.MinValue / prefix))
					{
						throw new ArgumentException(s + " does not fit in a Long");
					}
					return num * prefix;
				}
			}

			/// <summary>Convert a long integer to a string with traditional binary prefix.</summary>
			/// <param name="n">the value to be converted</param>
			/// <param name="unit">The unit, e.g. "B" for bytes.</param>
			/// <param name="decimalPlaces">The number of decimal places.</param>
			/// <returns>a string with traditional binary prefix.</returns>
			public static string Long2String(long n, string unit, int decimalPlaces)
			{
				if (unit == null)
				{
					unit = string.Empty;
				}
				//take care a special case
				if (n == long.MinValue)
				{
					return "-8 " + StringUtils.TraditionalBinaryPrefix.Exa.symbol + unit;
				}
				StringBuilder b = new StringBuilder();
				//take care negative numbers
				if (n < 0)
				{
					b.Append('-');
					n = -n;
				}
				if (n < StringUtils.TraditionalBinaryPrefix.Kilo.value)
				{
					//no prefix
					b.Append(n);
					return (unit.IsEmpty() ? b : b.Append(" ").Append(unit)).ToString();
				}
				else
				{
					//find traditional binary prefix
					int i = 0;
					for (; i < Values().Length && n >= Values()[i].value; i++)
					{
					}
					StringUtils.TraditionalBinaryPrefix prefix = Values()[i - 1];
					if ((n & prefix.bitMask) == 0)
					{
						//exact division
						b.Append(n >> prefix.bitShift);
					}
					else
					{
						string format = "%." + decimalPlaces + "f";
						string s = Format(format, n / (double)prefix.value);
						//check a special rounding up case
						if (s.StartsWith("1024"))
						{
							prefix = Values()[i];
							s = Format(format, n / (double)prefix.value);
						}
						b.Append(s);
					}
					return b.Append(' ').Append(prefix.symbol).Append(unit).ToString();
				}
			}
		}

		/// <summary>Escapes HTML Special characters present in the string.</summary>
		/// <param name="string"/>
		/// <returns>HTML Escaped String representation</returns>
		public static string EscapeHTML(string @string)
		{
			if (@string == null)
			{
				return null;
			}
			StringBuilder sb = new StringBuilder();
			bool lastCharacterWasSpace = false;
			char[] chars = @string.ToCharArray();
			foreach (char c in chars)
			{
				if (c == ' ')
				{
					if (lastCharacterWasSpace)
					{
						lastCharacterWasSpace = false;
						sb.Append("&nbsp;");
					}
					else
					{
						lastCharacterWasSpace = true;
						sb.Append(" ");
					}
				}
				else
				{
					lastCharacterWasSpace = false;
					switch (c)
					{
						case '<':
						{
							sb.Append("&lt;");
							break;
						}

						case '>':
						{
							sb.Append("&gt;");
							break;
						}

						case '&':
						{
							sb.Append("&amp;");
							break;
						}

						case '"':
						{
							sb.Append("&quot;");
							break;
						}

						default:
						{
							sb.Append(c);
							break;
						}
					}
				}
			}
			return sb.ToString();
		}

		/// <returns>a byte description of the given long interger value.</returns>
		public static string ByteDesc(long len)
		{
			return StringUtils.TraditionalBinaryPrefix.Long2String(len, "B", 2);
		}

		[System.ObsoleteAttribute(@"use StringUtils.format(""%.2f"", d).")]
		public static string LimitDecimalTo2(double d)
		{
			return Format("%.2f", d);
		}

		/// <summary>Concatenates strings, using a separator.</summary>
		/// <param name="separator">Separator to join with.</param>
		/// <param name="strings">Strings to join.</param>
		public static string Join<_T0>(CharSequence separator, IEnumerable<_T0> strings)
		{
			IEnumerator<object> i = strings.GetEnumerator();
			if (!i.HasNext())
			{
				return string.Empty;
			}
			StringBuilder sb = new StringBuilder(i.Next().ToString());
			while (i.HasNext())
			{
				sb.Append(separator);
				sb.Append(i.Next().ToString());
			}
			return sb.ToString();
		}

		/// <summary>Concatenates strings, using a separator.</summary>
		/// <param name="separator">to join with</param>
		/// <param name="strings">to join</param>
		/// <returns>the joined string</returns>
		public static string Join(CharSequence separator, string[] strings)
		{
			// Ideally we don't have to duplicate the code here if array is iterable.
			StringBuilder sb = new StringBuilder();
			bool first = true;
			foreach (string s in strings)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					sb.Append(separator);
				}
				sb.Append(s);
			}
			return sb.ToString();
		}

		/// <summary>Convert SOME_STUFF to SomeStuff</summary>
		/// <param name="s">input string</param>
		/// <returns>camelized string</returns>
		public static string Camelize(string s)
		{
			StringBuilder sb = new StringBuilder();
			string[] words = Split(StringUtils.ToLowerCase(s), EscapeChar, '_');
			foreach (string word in words)
			{
				sb.Append(StringUtils.Capitalize(word));
			}
			return sb.ToString();
		}

		/// <summary>
		/// Matches a template string against a pattern, replaces matched tokens with
		/// the supplied replacements, and returns the result.
		/// </summary>
		/// <remarks>
		/// Matches a template string against a pattern, replaces matched tokens with
		/// the supplied replacements, and returns the result.  The regular expression
		/// must use a capturing group.  The value of the first capturing group is used
		/// to look up the replacement.  If no replacement is found for the token, then
		/// it is replaced with the empty string.
		/// For example, assume template is "%foo%_%bar%_%baz%", pattern is "%(.*?)%",
		/// and replacements contains 2 entries, mapping "foo" to "zoo" and "baz" to
		/// "zaz".  The result returned would be "zoo__zaz".
		/// </remarks>
		/// <param name="template">String template to receive replacements</param>
		/// <param name="pattern">
		/// Pattern to match for identifying tokens, must use a capturing
		/// group
		/// </param>
		/// <param name="replacements">
		/// Map<String, String> mapping tokens identified by the
		/// capturing group to their replacement values
		/// </param>
		/// <returns>String template with replacements</returns>
		public static string ReplaceTokens(string template, Sharpen.Pattern pattern, IDictionary
			<string, string> replacements)
		{
			StringBuilder sb = new StringBuilder();
			Matcher matcher = pattern.Matcher(template);
			while (matcher.Find())
			{
				string replacement = replacements[matcher.Group(1)];
				if (replacement == null)
				{
					replacement = string.Empty;
				}
				matcher.AppendReplacement(sb, Matcher.QuoteReplacement(replacement));
			}
			matcher.AppendTail(sb);
			return sb.ToString();
		}

		/// <summary>Get stack trace for a given thread.</summary>
		public static string GetStackTrace(Sharpen.Thread t)
		{
			StackTraceElement[] stackTrace = t.GetStackTrace();
			StringBuilder str = new StringBuilder();
			foreach (StackTraceElement e in stackTrace)
			{
				str.Append(e.ToString() + "\n");
			}
			return str.ToString();
		}

		/// <summary>
		/// From a list of command-line arguments, remove both an option and the
		/// next argument.
		/// </summary>
		/// <param name="name">Name of the option to remove.  Example: -foo.</param>
		/// <param name="args">List of arguments.</param>
		/// <returns>
		/// null if the option was not found; the value of the
		/// option otherwise.
		/// </returns>
		/// <exception cref="System.ArgumentException">if the option's argument is not present
		/// 	</exception>
		public static string PopOptionWithArgument(string name, IList<string> args)
		{
			string val = null;
			for (IEnumerator<string> iter = args.GetEnumerator(); iter.HasNext(); )
			{
				string cur = iter.Next();
				if (cur.Equals("--"))
				{
					// stop parsing arguments when you see --
					break;
				}
				else
				{
					if (cur.Equals(name))
					{
						iter.Remove();
						if (!iter.HasNext())
						{
							throw new ArgumentException("option " + name + " requires 1 " + "argument.");
						}
						val = iter.Next();
						iter.Remove();
						break;
					}
				}
			}
			return val;
		}

		/// <summary>From a list of command-line arguments, remove an option.</summary>
		/// <param name="name">Name of the option to remove.  Example: -foo.</param>
		/// <param name="args">List of arguments.</param>
		/// <returns>true if the option was found and removed; false otherwise.</returns>
		public static bool PopOption(string name, IList<string> args)
		{
			for (IEnumerator<string> iter = args.GetEnumerator(); iter.HasNext(); )
			{
				string cur = iter.Next();
				if (cur.Equals("--"))
				{
					// stop parsing arguments when you see --
					break;
				}
				else
				{
					if (cur.Equals(name))
					{
						iter.Remove();
						return true;
					}
				}
			}
			return false;
		}

		/// <summary>
		/// From a list of command-line arguments, return the first non-option
		/// argument.
		/// </summary>
		/// <remarks>
		/// From a list of command-line arguments, return the first non-option
		/// argument.  Non-option arguments are those which either come after
		/// a double dash (--) or do not start with a dash.
		/// </remarks>
		/// <param name="args">List of arguments.</param>
		/// <returns>The first non-option argument, or null if there were none.</returns>
		public static string PopFirstNonOption(IList<string> args)
		{
			for (IEnumerator<string> iter = args.GetEnumerator(); iter.HasNext(); )
			{
				string cur = iter.Next();
				if (cur.Equals("--"))
				{
					if (!iter.HasNext())
					{
						return null;
					}
					cur = iter.Next();
					iter.Remove();
					return cur;
				}
				else
				{
					if (!cur.StartsWith("-"))
					{
						iter.Remove();
						return cur;
					}
				}
			}
			return null;
		}

		/// <summary>
		/// Converts all of the characters in this String to lower case with
		/// Locale.ENGLISH.
		/// </summary>
		/// <param name="str">string to be converted</param>
		/// <returns>the str, converted to lowercase.</returns>
		public static string ToLowerCase(string str)
		{
			return str.ToLower(Sharpen.Extensions.GetEnglishCulture());
		}

		/// <summary>
		/// Converts all of the characters in this String to upper case with
		/// Locale.ENGLISH.
		/// </summary>
		/// <param name="str">string to be converted</param>
		/// <returns>the str, converted to uppercase.</returns>
		public static string ToUpperCase(string str)
		{
			return str.ToUpper(Sharpen.Extensions.GetEnglishCulture());
		}

		/// <summary>Compare strings locale-freely by using String#equalsIgnoreCase.</summary>
		/// <param name="s1">Non-null string to be converted</param>
		/// <param name="s2">string to be converted</param>
		/// <returns>the str, converted to uppercase.</returns>
		public static bool EqualsIgnoreCase(string s1, string s2)
		{
			Preconditions.CheckNotNull(s1);
			// don't check non-null against s2 to make the semantics same as
			// s1.equals(s2)
			return Sharpen.Runtime.EqualsIgnoreCase(s1, s2);
		}
	}
}
