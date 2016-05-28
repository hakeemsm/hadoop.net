using System;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>The interface for the messages that can come up from the child.</summary>
	/// <remarks>
	/// The interface for the messages that can come up from the child. All of these
	/// calls are asynchronous and return before the message has been processed.
	/// </remarks>
	internal interface UpwardProtocol<K, V>
		where K : WritableComparable
		where V : Writable
	{
		/// <summary>Output a record from the child.</summary>
		/// <param name="key">the record's key</param>
		/// <param name="value">the record's value</param>
		/// <exception cref="System.IO.IOException"/>
		void Output(K key, V value);

		/// <summary>
		/// Map functions where the application has defined a partition function
		/// output records along with their partition.
		/// </summary>
		/// <param name="reduce">the reduce to send this record to</param>
		/// <param name="key">the record's key</param>
		/// <param name="value">the record's value</param>
		/// <exception cref="System.IO.IOException"/>
		void PartitionedOutput(int reduce, K key, V value);

		/// <summary>Update the task's status message</summary>
		/// <param name="msg">the string to display to the user</param>
		/// <exception cref="System.IO.IOException"/>
		void Status(string msg);

		/// <summary>Report making progress (and the current progress)</summary>
		/// <param name="progress">the current progress (0.0 to 1.0)</param>
		/// <exception cref="System.IO.IOException"/>
		void Progress(float progress);

		/// <summary>
		/// Report that the application has finished processing all inputs
		/// successfully.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		void Done();

		/// <summary>Report that the application or more likely communication failed.</summary>
		/// <param name="e"/>
		void Failed(Exception e);

		/// <summary>Register a counter with the given id and group/name.</summary>
		/// <param name="group">counter group</param>
		/// <param name="name">counter name</param>
		/// <exception cref="System.IO.IOException"/>
		void RegisterCounter(int id, string group, string name);

		/// <summary>Increment the value of a registered counter.</summary>
		/// <param name="id">counter id of the registered counter</param>
		/// <param name="amount">increment for the counter value</param>
		/// <exception cref="System.IO.IOException"/>
		void IncrementCounter(int id, long amount);

		/// <summary>Handles authentication response from client.</summary>
		/// <remarks>
		/// Handles authentication response from client.
		/// It must notify the threads waiting for authentication response.
		/// </remarks>
		/// <param name="digest"/>
		/// <returns>true if authentication is successful</returns>
		/// <exception cref="System.IO.IOException"/>
		bool Authenticate(string digest);
	}
}
