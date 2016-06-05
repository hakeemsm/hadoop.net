using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web.Resources
{
	/// <summary>Base class of parameters.</summary>
	public abstract class Param<T, D>
		where D : Param.Domain<T>
	{
		internal const string Null = "null";

		private sealed class _IComparer_30 : IComparer<Org.Apache.Hadoop.Hdfs.Web.Resources.Param
			<object, object>>
		{
			public _IComparer_30()
			{
			}

			public int Compare<_T0, _T1>(Org.Apache.Hadoop.Hdfs.Web.Resources.Param<_T0> left
				, Org.Apache.Hadoop.Hdfs.Web.Resources.Param<_T1> right)
			{
				return string.CompareOrdinal(left.GetName(), right.GetName());
			}
		}

		internal static readonly IComparer<Org.Apache.Hadoop.Hdfs.Web.Resources.Param<object
			, object>> NameCmp = new _IComparer_30();

		/// <summary>Convert the parameters to a sorted String.</summary>
		/// <param name="separator">URI parameter separator character</param>
		/// <param name="parameters">parameters to encode into a string</param>
		/// <returns>the encoded URI string</returns>
		public static string ToSortedString(string separator, params Org.Apache.Hadoop.Hdfs.Web.Resources.Param
			<object, object>[] parameters)
		{
			Arrays.Sort(parameters, NameCmp);
			StringBuilder b = new StringBuilder();
			try
			{
				foreach (Org.Apache.Hadoop.Hdfs.Web.Resources.Param<object, object> p in parameters)
				{
					if (p.GetValue() != null)
					{
						b.Append(separator).Append(URLEncoder.Encode(p.GetName(), "UTF-8") + "=" + URLEncoder
							.Encode(p.GetValueString(), "UTF-8"));
					}
				}
			}
			catch (UnsupportedEncodingException e)
			{
				// Sane systems know about UTF-8, so this should never happen.
				throw new RuntimeException(e);
			}
			return b.ToString();
		}

		/// <summary>The domain of the parameter.</summary>
		internal readonly D domain;

		/// <summary>The actual parameter value.</summary>
		internal readonly T value;

		internal Param(D domain, T value)
		{
			this.domain = domain;
			this.value = value;
		}

		/// <returns>the parameter value.</returns>
		public T GetValue()
		{
			return value;
		}

		/// <returns>the parameter value as a string</returns>
		public abstract string GetValueString();

		/// <returns>the parameter name.</returns>
		public abstract string GetName();

		public override string ToString()
		{
			return GetName() + "=" + value;
		}

		/// <summary>Base class of parameter domains.</summary>
		internal abstract class Domain<T>
		{
			/// <summary>Parameter name.</summary>
			internal readonly string paramName;

			internal Domain(string paramName)
			{
				this.paramName = paramName;
			}

			/// <returns>the parameter name.</returns>
			public string GetParamName()
			{
				return paramName;
			}

			/// <returns>a string description of the domain of the parameter.</returns>
			public abstract string GetDomain();

			/// <returns>the parameter value represented by the string.</returns>
			internal abstract T Parse(string str);

			/// <summary>Parse the given string.</summary>
			/// <returns>the parameter value represented by the string.</returns>
			public T Parse(string varName, string str)
			{
				try
				{
					return str != null && str.Trim().Length > 0 ? Parse(str) : null;
				}
				catch (Exception e)
				{
					throw new ArgumentException("Failed to parse \"" + str + "\" for the parameter " 
						+ varName + ".  The value must be in the domain " + GetDomain(), e);
				}
			}
		}
	}
}
