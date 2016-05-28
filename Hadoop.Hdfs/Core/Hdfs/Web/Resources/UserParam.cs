using System;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>User parameter.</summary>
	public class UserParam : StringParam
	{
		/// <summary>Parameter name.</summary>
		public const string Name = "user.name";

		/// <summary>Default parameter value.</summary>
		public const string Default = string.Empty;

		private static StringParam.Domain domain = new StringParam.Domain(Name, Sharpen.Pattern
			.Compile(DFSConfigKeys.DfsWebhdfsUserPatternDefault));

		[VisibleForTesting]
		public static StringParam.Domain GetUserPatternDomain()
		{
			return domain;
		}

		[VisibleForTesting]
		public static void SetUserPatternDomain(StringParam.Domain dm)
		{
			domain = dm;
		}

		public static void SetUserPattern(string pattern)
		{
			domain = new StringParam.Domain(Name, Sharpen.Pattern.Compile(pattern));
		}

		private static string ValidateLength(string str)
		{
			if (str == null)
			{
				throw new ArgumentException(MessageFormat.Format("Parameter [{0}], cannot be NULL"
					, Name));
			}
			int len = str.Length;
			if (len < 1)
			{
				throw new ArgumentException(MessageFormat.Format("Parameter [{0}], it's length must be at least 1"
					, Name));
			}
			return str;
		}

		/// <summary>Constructor.</summary>
		/// <param name="str">a string representation of the parameter value.</param>
		public UserParam(string str)
			: base(domain, str == null || str.Equals(Default) ? null : ValidateLength(str))
		{
		}

		/// <summary>Construct an object from a UGI.</summary>
		public UserParam(UserGroupInformation ugi)
			: this(ugi.GetShortUserName())
		{
		}

		public override string GetName()
		{
			return Name;
		}
	}
}
