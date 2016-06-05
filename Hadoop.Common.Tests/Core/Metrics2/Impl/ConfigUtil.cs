using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Configuration;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Helpers for config tests and debugging</summary>
	internal class ConfigUtil
	{
		internal static void Dump(Org.Apache.Commons.Configuration.Configuration c)
		{
			Dump(null, c, System.Console.Out);
		}

		internal static void Dump(string header, Org.Apache.Commons.Configuration.Configuration
			 c)
		{
			Dump(header, c, System.Console.Out);
		}

		internal static void Dump(string header, Org.Apache.Commons.Configuration.Configuration
			 c, TextWriter @out)
		{
			PropertiesConfiguration p = new PropertiesConfiguration();
			p.Copy(c);
			if (header != null)
			{
				@out.WriteLine(header);
			}
			try
			{
				p.Save(@out);
			}
			catch (Exception e)
			{
				throw new RuntimeException("Error saving config", e);
			}
		}

		internal static void AssertEq(Org.Apache.Commons.Configuration.Configuration expected
			, Org.Apache.Commons.Configuration.Configuration actual)
		{
			// Check that the actual config contains all the properties of the expected
			for (IEnumerator<object> it = expected.GetKeys(); it.HasNext(); )
			{
				string key = (string)it.Next();
				Assert.True("actual should contain " + key, actual.ContainsKey(
					key));
				Assert.Equal("value of " + key, expected.GetProperty(key), actual
					.GetProperty(key));
			}
			// Check that the actual config has no extra properties
			for (IEnumerator<object> it_1 = actual.GetKeys(); it_1.HasNext(); )
			{
				string key = (string)it_1.Next();
				Assert.True("expected should contain " + key, expected.ContainsKey
					(key));
			}
		}
	}
}
