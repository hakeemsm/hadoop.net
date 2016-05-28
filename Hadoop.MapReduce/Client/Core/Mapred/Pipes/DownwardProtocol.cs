using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>The abstract description of the downward (from Java to C++) Pipes protocol.
	/// 	</summary>
	/// <remarks>
	/// The abstract description of the downward (from Java to C++) Pipes protocol.
	/// All of these calls are asynchronous and return before the message has been
	/// processed.
	/// </remarks>
	internal interface DownwardProtocol<K, V>
		where K : WritableComparable
		where V : Writable
	{
		/// <summary>request authentication</summary>
		/// <exception cref="System.IO.IOException"/>
		void Authenticate(string digest, string challenge);

		/// <summary>Start communication</summary>
		/// <exception cref="System.IO.IOException"/>
		void Start();

		/// <summary>Set the JobConf for the task.</summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		void SetJobConf(JobConf conf);

		/// <summary>Set the input types for Maps.</summary>
		/// <param name="keyType">the name of the key's type</param>
		/// <param name="valueType">the name of the value's type</param>
		/// <exception cref="System.IO.IOException"/>
		void SetInputTypes(string keyType, string valueType);

		/// <summary>Run a map task in the child.</summary>
		/// <param name="split">The input split for this map.</param>
		/// <param name="numReduces">The number of reduces for this job.</param>
		/// <param name="pipedInput">Is the input coming from Java?</param>
		/// <exception cref="System.IO.IOException"/>
		void RunMap(InputSplit split, int numReduces, bool pipedInput);

		/// <summary>For maps with pipedInput, the key/value pairs are sent via this messaage.
		/// 	</summary>
		/// <param name="key">The record's key</param>
		/// <param name="value">The record's value</param>
		/// <exception cref="System.IO.IOException"/>
		void MapItem(K key, V value);

		/// <summary>Run a reduce task in the child</summary>
		/// <param name="reduce">the index of the reduce (0 .. numReduces - 1)</param>
		/// <param name="pipedOutput">is the output being sent to Java?</param>
		/// <exception cref="System.IO.IOException"/>
		void RunReduce(int reduce, bool pipedOutput);

		/// <summary>The reduce should be given a new key</summary>
		/// <param name="key">the new key</param>
		/// <exception cref="System.IO.IOException"/>
		void ReduceKey(K key);

		/// <summary>The reduce should be given a new value</summary>
		/// <param name="value">the new value</param>
		/// <exception cref="System.IO.IOException"/>
		void ReduceValue(V value);

		/// <summary>
		/// The task has no more input coming, but it should finish processing it's
		/// input.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		void EndOfInput();

		/// <summary>The task should stop as soon as possible, because something has gone wrong.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		void Abort();

		/// <summary>Flush the data through any buffers.</summary>
		/// <exception cref="System.IO.IOException"/>
		void Flush();

		/// <summary>Close the connection.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		void Close();
	}
}
