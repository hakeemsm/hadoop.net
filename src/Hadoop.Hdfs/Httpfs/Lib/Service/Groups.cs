using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Lib.Service
{
	public interface Groups
	{
		/// <exception cref="System.IO.IOException"/>
		IList<string> GetGroups(string user);
	}
}
