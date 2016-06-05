using System;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>
	/// Stringifier interface offers two methods to convert an object
	/// to a string representation and restore the object given its
	/// string representation.
	/// </summary>
	/// <?/>
	public interface Stringifier<T> : IDisposable
	{
		/// <summary>Converts the object to a string representation</summary>
		/// <param name="obj">the object to convert</param>
		/// <returns>the string representation of the object</returns>
		/// <exception cref="System.IO.IOException">if the object cannot be converted</exception>
		string ToString(T obj);

		/// <summary>Restores the object from its string representation.</summary>
		/// <param name="str">the string representation of the object</param>
		/// <returns>restored object</returns>
		/// <exception cref="System.IO.IOException">if the object cannot be restored</exception>
		T FromString(string str);

		/// <summary>Closes this object.</summary>
		/// <exception cref="System.IO.IOException">if an I/O error occurs</exception>
		void Close();
	}
}
