using System;
using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	/// <summary>
	/// This type has been deprecated in favor of
	/// <see cref="Org.Apache.Hadoop.Util.ApplicationClassLoader"/>
	/// . All new uses of
	/// ApplicationClassLoader should use that type instead.
	/// </summary>
	public class ApplicationClassLoader : Org.Apache.Hadoop.Util.ApplicationClassLoader
	{
		public ApplicationClassLoader(Uri[] urls, ClassLoader parent, IList<string> systemClasses
			)
			: base(urls, parent, systemClasses)
		{
		}

		/// <exception cref="System.UriFormatException"/>
		public ApplicationClassLoader(string classpath, ClassLoader parent, IList<string>
			 systemClasses)
			: base(classpath, parent, systemClasses)
		{
		}
	}
}
