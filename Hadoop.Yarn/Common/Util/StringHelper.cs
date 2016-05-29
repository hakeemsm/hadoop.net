using System.Collections.Generic;
using System.Text;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>Common string manipulation helpers</summary>
	public sealed class StringHelper
	{
		public static readonly Joiner SsvJoiner = Joiner.On(' ');

		public static readonly Joiner CsvJoiner = Joiner.On(',');

		public static readonly Joiner Joiner = Joiner.On(string.Empty);

		public static readonly Joiner Joiner = Joiner.On('_');

		public static readonly Joiner PathJoiner = Joiner.On('/');

		public static readonly Joiner PathArgJoiner = Joiner.On("/:");

		public static readonly Joiner DotJoiner = Joiner.On('.');

		public static readonly Splitter SsvSplitter = Splitter.On(' ').OmitEmptyStrings()
			.TrimResults();

		public static readonly Splitter Splitter = Splitter.On('_').TrimResults();

		private static readonly Sharpen.Pattern AbsUrlRe = Sharpen.Pattern.Compile("^(?:\\w+:)?//"
			);

		// Common joiners to avoid per join creation of joiners
		/// <summary>Join on space.</summary>
		/// <param name="args">to join</param>
		/// <returns>args joined by space</returns>
		public static string Sjoin(params object[] args)
		{
			return SsvJoiner.Join(args);
		}

		/// <summary>Join on comma.</summary>
		/// <param name="args">to join</param>
		/// <returns>args joined by comma</returns>
		public static string Cjoin(params object[] args)
		{
			return CsvJoiner.Join(args);
		}

		/// <summary>Join on dot</summary>
		/// <param name="args">to join</param>
		/// <returns>args joined by dot</returns>
		public static string Djoin(params object[] args)
		{
			return DotJoiner.Join(args);
		}

		/// <summary>Join on underscore</summary>
		/// <param name="args">to join</param>
		/// <returns>args joined underscore</returns>
		public static string _join(params object[] args)
		{
			return Joiner.Join(args);
		}

		/// <summary>Join on slash</summary>
		/// <param name="args">to join</param>
		/// <returns>args joined with slash</returns>
		public static string Pjoin(params object[] args)
		{
			return PathJoiner.Join(args);
		}

		/// <summary>Join on slash and colon (e.g., path args in routing spec)</summary>
		/// <param name="args">to join</param>
		/// <returns>args joined with /:</returns>
		public static string Pajoin(params object[] args)
		{
			return PathArgJoiner.Join(args);
		}

		/// <summary>Join without separator</summary>
		/// <param name="args"/>
		/// <returns>joined args with no separator</returns>
		public static string Join(params object[] args)
		{
			return Joiner.Join(args);
		}

		/// <summary>Join with a separator</summary>
		/// <param name="sep">the separator</param>
		/// <param name="args">to join</param>
		/// <returns>args joined with a separator</returns>
		public static string Joins(string sep, params object[] args)
		{
			return Joiner.On(sep).Join(args);
		}

		/// <summary>Split on space and trim results.</summary>
		/// <param name="s">the string to split</param>
		/// <returns>an iterable of strings</returns>
		public static IEnumerable<string> Split(CharSequence s)
		{
			return SsvSplitter.Split(s);
		}

		/// <summary>Split on _ and trim results</summary>
		/// <param name="s">the string to split</param>
		/// <returns>an iterable of strings</returns>
		public static IEnumerable<string> _split(CharSequence s)
		{
			return Splitter.Split(s);
		}

		/// <summary>Check whether a url is absolute or note</summary>
		/// <param name="url">to check</param>
		/// <returns>true if url starts with scheme:// or //</returns>
		public static bool IsAbsUrl(CharSequence url)
		{
			return AbsUrlRe.Matcher(url).Find();
		}

		/// <summary>Join url components</summary>
		/// <param name="pathPrefix">for relative urls</param>
		/// <param name="args">url components to join</param>
		/// <returns>an url string</returns>
		public static string Ujoin(string pathPrefix, params string[] args)
		{
			StringBuilder sb = new StringBuilder();
			bool first = true;
			foreach (string part in args)
			{
				if (first)
				{
					first = false;
					if (part.StartsWith("#") || IsAbsUrl(part))
					{
						sb.Append(part);
					}
					else
					{
						Uappend(sb, pathPrefix);
						Uappend(sb, part);
					}
				}
				else
				{
					Uappend(sb, part);
				}
			}
			return sb.ToString();
		}

		private static void Uappend(StringBuilder sb, string part)
		{
			if ((sb.Length <= 0 || sb[sb.Length - 1] != '/') && !part.StartsWith("/"))
			{
				sb.Append('/');
			}
			sb.Append(part);
		}

		public static string Percent(double value)
		{
			return string.Format("%.2f", value * 100);
		}
	}
}
