using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp
{
	/// <summary>A light-weight JSON rendering interface</summary>
	public interface ToJSON
	{
		void ToJSON(PrintWriter @out);
	}
}
