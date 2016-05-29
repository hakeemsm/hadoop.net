using System;
using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>Yarn internal application-related utilities</summary>
	public class Apps
	{
		public const string App = "application";

		public const string Id = "ID";

		public static ApplicationId ToAppID(string aid)
		{
			IEnumerator<string> it = StringHelper._split(aid).GetEnumerator();
			return ToAppID(App, aid, it);
		}

		public static ApplicationId ToAppID(string prefix, string s, IEnumerator<string> 
			it)
		{
			if (!it.HasNext() || !it.Next().Equals(prefix))
			{
				ThrowParseException(StringHelper.Sjoin(prefix, Id), s);
			}
			ShouldHaveNext(prefix, s, it);
			ApplicationId appId = ApplicationId.NewInstance(long.Parse(it.Next()), System.Convert.ToInt32
				(it.Next()));
			return appId;
		}

		public static void ShouldHaveNext(string prefix, string s, IEnumerator<string> it
			)
		{
			if (!it.HasNext())
			{
				ThrowParseException(StringHelper.Sjoin(prefix, Id), s);
			}
		}

		public static void ThrowParseException(string name, string s)
		{
			throw new YarnRuntimeException(StringHelper.Join("Error parsing ", name, ": ", s)
				);
		}

		public static void SetEnvFromInputString(IDictionary<string, string> env, string 
			envString, string classPathSeparator)
		{
			if (envString != null && envString.Length > 0)
			{
				string[] childEnvs = envString.Split(",");
				Sharpen.Pattern p = Sharpen.Pattern.Compile(Shell.GetEnvironmentVariableRegex());
				foreach (string cEnv in childEnvs)
				{
					string[] parts = cEnv.Split("=");
					// split on '='
					Matcher m = p.Matcher(parts[1]);
					StringBuilder sb = new StringBuilder();
					while (m.Find())
					{
						string var = m.Group(1);
						// replace $env with the child's env constructed by tt's
						string replace = env[var];
						// if this key is not configured by the tt for the child .. get it
						// from the tt's env
						if (replace == null)
						{
							replace = Runtime.Getenv(var);
						}
						// the env key is note present anywhere .. simply set it
						if (replace == null)
						{
							replace = string.Empty;
						}
						m.AppendReplacement(sb, Matcher.QuoteReplacement(replace));
					}
					m.AppendTail(sb);
					AddToEnvironment(env, parts[0], sb.ToString(), classPathSeparator);
				}
			}
		}

		/// <summary>
		/// This older version of this method is kept around for compatibility
		/// because downstream frameworks like Spark and Tez have been using it.
		/// </summary>
		/// <remarks>
		/// This older version of this method is kept around for compatibility
		/// because downstream frameworks like Spark and Tez have been using it.
		/// Downstream frameworks are expected to move off of it.
		/// </remarks>
		[Obsolete]
		public static void SetEnvFromInputString(IDictionary<string, string> env, string 
			envString)
		{
			SetEnvFromInputString(env, envString, FilePath.pathSeparator);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static void AddToEnvironment(IDictionary<string, string> environment, string
			 variable, string value, string classPathSeparator)
		{
			string val = environment[variable];
			if (val == null)
			{
				val = value;
			}
			else
			{
				val = val + classPathSeparator + value;
			}
			environment[StringInterner.WeakIntern(variable)] = StringInterner.WeakIntern(val);
		}

		/// <summary>
		/// This older version of this method is kept around for compatibility
		/// because downstream frameworks like Spark and Tez have been using it.
		/// </summary>
		/// <remarks>
		/// This older version of this method is kept around for compatibility
		/// because downstream frameworks like Spark and Tez have been using it.
		/// Downstream frameworks are expected to move off of it.
		/// </remarks>
		[Obsolete]
		public static void AddToEnvironment(IDictionary<string, string> environment, string
			 variable, string value)
		{
			AddToEnvironment(environment, variable, value, FilePath.pathSeparator);
		}

		public static string CrossPlatformify(string var)
		{
			return ApplicationConstants.ParameterExpansionLeft + var + ApplicationConstants.ParameterExpansionRight;
		}
	}
}
