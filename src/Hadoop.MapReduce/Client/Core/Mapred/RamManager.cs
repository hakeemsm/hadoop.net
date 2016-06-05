using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary><code>RamManager</code> manages a memory pool of a configured limit.</summary>
	internal interface RamManager
	{
		/// <summary>Reserve memory for data coming through the given input-stream.</summary>
		/// <param name="requestedSize">size of memory requested</param>
		/// <param name="in">input stream</param>
		/// <exception cref="System.Exception"/>
		/// <returns>
		/// <code>true</code> if memory was allocated immediately,
		/// else <code>false</code>
		/// </returns>
		bool Reserve(int requestedSize, InputStream @in);

		/// <summary>Return memory to the pool.</summary>
		/// <param name="requestedSize">size of memory returned to the pool</param>
		void Unreserve(int requestedSize);
	}
}
